#!/usr/bin/env escript
%%% @doc PyBuffer example - demonstrates zero-copy WSGI input buffer.
%%%
%%% This example shows how to use py_buffer for streaming HTTP body
%%% data from Erlang to Python, suitable for WSGI/ASGI input.
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/py_buffer_example.erl

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== PyBuffer Zero-Copy WSGI Input Demo ===~n~n"),

    %% Demo 1: Basic buffer usage
    basic_buffer_demo(),

    %% Demo 2: Simulated HTTP body streaming
    http_body_demo(),

    %% Demo 3: File-like interface
    file_like_demo(),

    %% Demo 4: Zero-copy memoryview access
    memoryview_demo(),

    %% Demo 5: Line iteration
    line_iteration_demo(),

    io:format("=== Done ===~n~n"),
    ok.

basic_buffer_demo() ->
    io:format("--- Basic Buffer Demo ---~n~n"),

    %% Create a buffer
    io:format("Creating buffer...~n"),
    {ok, Buf} = py_buffer:new(),

    %% Write some data
    io:format("Writing data chunks...~n"),
    ok = py_buffer:write(Buf, <<"Hello, ">>),
    ok = py_buffer:write(Buf, <<"World!">>),

    %% Close to signal EOF
    ok = py_buffer:close(Buf),
    io:format("Buffer closed (EOF signaled)~n"),

    %% Pass to Python and read
    io:format("Reading from Python...~n"),
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
def read_all(buf):
    data = buf.read()
    print(f'  Read {len(data)} bytes: {data}')
    return data
">>),

    {ok, Data} = py:eval(Ctx, <<"read_all(buf)">>, #{<<"buf">> => Buf}),
    io:format("Erlang received: ~p~n~n", [Data]),
    ok.

http_body_demo() ->
    io:format("--- HTTP Body Streaming Demo ---~n~n"),

    %% Simulate receiving a JSON POST body
    Body = <<"{\"user\": \"alice\", \"action\": \"login\", \"timestamp\": 1234567890}">>,
    ContentLength = byte_size(Body),

    io:format("Simulating HTTP POST with ~p byte body~n", [ContentLength]),

    %% Create buffer with known content length (pre-allocates)
    {ok, Buf} = py_buffer:new(ContentLength),

    %% Write the body (could be in chunks)
    ok = py_buffer:write(Buf, Body),
    ok = py_buffer:close(Buf),

    %% Build WSGI-like environ
    Ctx = py:context(1),
    ok = py:exec(Ctx, <<"
import json

def handle_request(environ):
    '''Simulate WSGI request handler.'''
    method = environ.get('REQUEST_METHOD', 'GET')
    path = environ.get('PATH_INFO', '/')
    content_type = environ.get('CONTENT_TYPE', '')

    print(f'  {method} {path}')
    print(f'  Content-Type: {content_type}')

    # Read body from wsgi.input (PyBuffer)
    wsgi_input = environ.get('wsgi.input')
    if wsgi_input:
        body = wsgi_input.read()
        print(f'  Body ({len(body)} bytes): {body[:50]}...' if len(body) > 50 else f'  Body: {body}')

        if content_type == 'application/json':
            data = json.loads(body)
            return {'status': 'ok', 'user': data.get('user')}

    return {'status': 'ok'}
">>),

    Environ = #{
        <<"REQUEST_METHOD">> => <<"POST">>,
        <<"PATH_INFO">> => <<"/api/login">>,
        <<"CONTENT_TYPE">> => <<"application/json">>,
        <<"CONTENT_LENGTH">> => integer_to_binary(ContentLength),
        <<"wsgi.input">> => Buf
    },

    {ok, Result} = py:eval(Ctx, <<"handle_request(environ)">>, #{<<"environ">> => Environ}),
    io:format("Response: ~p~n~n", [Result]),
    ok.

file_like_demo() ->
    io:format("--- File-Like Interface Demo ---~n~n"),

    Ctx = py:context(1),

    %% Create buffer with multiple lines
    {ok, Buf} = py_buffer:new(),
    ok = py_buffer:write(Buf, <<"Name: Alice\n">>),
    ok = py_buffer:write(Buf, <<"Email: alice@example.com\n">>),
    ok = py_buffer:write(Buf, <<"Role: Admin\n">>),
    ok = py_buffer:close(Buf),

    ok = py:exec(Ctx, <<"
def demonstrate_file_methods(buf):
    '''Show file-like methods.'''
    print('  File-like properties:')
    print(f'    readable(): {buf.readable()}')
    print(f'    writable(): {buf.writable()}')
    print(f'    seekable(): {buf.seekable()}')
    print(f'    len(buf): {len(buf)}')
    print()

    # Read first line
    line1 = buf.readline()
    print(f'  readline(): {line1}')

    # Current position
    pos = buf.tell()
    print(f'  tell(): {pos}')

    # Seek back to start
    buf.seek(0)
    print(f'  seek(0), tell(): {buf.tell()}')

    # Read all remaining
    rest = buf.read()
    print(f'  read(): {rest[:30]}...')

    return 'done'
">>),

    {ok, _} = py:eval(Ctx, <<"demonstrate_file_methods(buf)">>, #{<<"buf">> => Buf}),
    io:format("~n"),
    ok.

memoryview_demo() ->
    io:format("--- Zero-Copy Memoryview Demo ---~n~n"),

    Ctx = py:context(1),

    %% Create buffer with binary data
    Data = <<"HEADER:12345:PAYLOAD:abcdefghijklmnopqrstuvwxyz:END">>,
    {ok, Buf} = py_buffer:new(byte_size(Data)),
    ok = py_buffer:write(Buf, Data),
    ok = py_buffer:close(Buf),

    ok = py:exec(Ctx, <<"
def zero_copy_parse(buf):
    '''Demonstrate zero-copy access via memoryview.'''

    # Get memoryview - no data copying!
    mv = memoryview(buf)
    print(f'  memoryview created, {len(mv)} bytes')
    print(f'  readonly: {mv.readonly}')
    print(f'  ndim: {mv.ndim}')

    # Find colon positions using find (uses memchr internally)
    data_bytes = bytes(mv)  # Only for find, still efficient

    # Parse header
    first_colon = buf.find(b':')
    header = bytes(mv[:first_colon])
    print(f'  Header: {header}')

    # Find PAYLOAD section
    payload_start = buf.find(b'PAYLOAD:') + 8
    payload_end = buf.find(b':END')
    payload = bytes(mv[payload_start:payload_end])
    print(f'  Payload: {payload}')

    # Release memoryview
    mv.release()
    print('  memoryview released')

    return payload
">>),

    {ok, Payload} = py:eval(Ctx, <<"zero_copy_parse(buf)">>, #{<<"buf">> => Buf}),
    io:format("Extracted payload: ~p~n~n", [Payload]),
    ok.

line_iteration_demo() ->
    io:format("--- Line Iteration Demo ---~n~n"),

    Ctx = py:context(1),

    %% Create buffer with CSV-like data
    {ok, Buf} = py_buffer:new(),
    ok = py_buffer:write(Buf, <<"id,name,score\n">>),
    ok = py_buffer:write(Buf, <<"1,Alice,95\n">>),
    ok = py_buffer:write(Buf, <<"2,Bob,87\n">>),
    ok = py_buffer:write(Buf, <<"3,Charlie,92\n">>),
    ok = py_buffer:close(Buf),

    ok = py:exec(Ctx, <<"
def process_csv(buf):
    '''Iterate over lines like a file.'''
    records = []
    header = None

    for line in buf:
        line = line.strip()
        if not line:
            continue

        parts = line.decode().split(',')

        if header is None:
            header = parts
            print(f'  Header: {header}')
        else:
            record = dict(zip(header, parts))
            records.append(record)
            print(f'  Record: {record}')

    return records
">>),

    {ok, Records} = py:eval(Ctx, <<"process_csv(buf)">>, #{<<"buf">> => Buf}),
    io:format("Parsed ~p records~n~n", [length(Records)]),
    ok.
