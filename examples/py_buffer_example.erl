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

    %% Demo 6: Async I/O
    asyncio_demo(),

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
    ok = py:exec(Ctx, <<
        "\n"
        "def read_all(buf):\n"
        "    data = buf.read()\n"
        "    print(f'  Read {len(data)} bytes: {data}')\n"
        "    return data\n"
    >>),

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
    ok = py:exec(Ctx, <<
        "\n"
        "import json\n"
        "\n"
        "def handle_request(environ):\n"
        "    '''Simulate WSGI request handler.'''\n"
        "    method = environ.get('REQUEST_METHOD', 'GET')\n"
        "    path = environ.get('PATH_INFO', '/')\n"
        "    content_type = environ.get('CONTENT_TYPE', '')\n"
        "\n"
        "    print(f'  {method} {path}')\n"
        "    print(f'  Content-Type: {content_type}')\n"
        "\n"
        "    # Read body from wsgi.input (PyBuffer)\n"
        "    wsgi_input = environ.get('wsgi.input')\n"
        "    if wsgi_input:\n"
        "        body = wsgi_input.read()\n"
        "        print(f'  Body ({len(body)} bytes): {body[:50]}...' if len(body) > 50 else f'  Body: {body}')\n"
        "\n"
        "        if content_type == 'application/json':\n"
        "            data = json.loads(body)\n"
        "            return {'status': 'ok', 'user': data.get('user')}\n"
        "\n"
        "    return {'status': 'ok'}\n"
    >>),

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

    ok = py:exec(Ctx, <<
        "\n"
        "def demonstrate_file_methods(buf):\n"
        "    '''Show file-like methods.'''\n"
        "    print('  File-like properties:')\n"
        "    print(f'    readable(): {buf.readable()}')\n"
        "    print(f'    writable(): {buf.writable()}')\n"
        "    print(f'    seekable(): {buf.seekable()}')\n"
        "    print(f'    len(buf): {len(buf)}')\n"
        "    print()\n"
        "\n"
        "    # Read first line\n"
        "    line1 = buf.readline()\n"
        "    print(f'  readline(): {line1}')\n"
        "\n"
        "    # Current position\n"
        "    pos = buf.tell()\n"
        "    print(f'  tell(): {pos}')\n"
        "\n"
        "    # Seek back to start\n"
        "    buf.seek(0)\n"
        "    print(f'  seek(0), tell(): {buf.tell()}')\n"
        "\n"
        "    # Read all remaining\n"
        "    rest = buf.read()\n"
        "    print(f'  read(): {rest[:30]}...')\n"
        "\n"
        "    return 'done'\n"
    >>),

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

    ok = py:exec(Ctx, <<
        "\n"
        "def zero_copy_parse(buf):\n"
        "    '''Demonstrate zero-copy access via memoryview.'''\n"
        "\n"
        "    # Get memoryview - no data copying!\n"
        "    mv = memoryview(buf)\n"
        "    print(f'  memoryview created, {len(mv)} bytes')\n"
        "    print(f'  readonly: {mv.readonly}')\n"
        "    print(f'  ndim: {mv.ndim}')\n"
        "\n"
        "    # Find colon positions using find (uses memchr internally)\n"
        "    data_bytes = bytes(mv)  # Only for find, still efficient\n"
        "\n"
        "    # Parse header\n"
        "    first_colon = buf.find(b':')\n"
        "    header = bytes(mv[:first_colon])\n"
        "    print(f'  Header: {header}')\n"
        "\n"
        "    # Find PAYLOAD section\n"
        "    payload_start = buf.find(b'PAYLOAD:') + 8\n"
        "    payload_end = buf.find(b':END')\n"
        "    payload = bytes(mv[payload_start:payload_end])\n"
        "    print(f'  Payload: {payload}')\n"
        "\n"
        "    # Release memoryview\n"
        "    mv.release()\n"
        "    print('  memoryview released')\n"
        "\n"
        "    return payload\n"
    >>),

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

    ok = py:exec(Ctx, <<
        "\n"
        "def process_csv(buf):\n"
        "    '''Iterate over lines like a file.'''\n"
        "    records = []\n"
        "    header = None\n"
        "\n"
        "    for line in buf:\n"
        "        line = line.strip()\n"
        "        if not line:\n"
        "            continue\n"
        "\n"
        "        parts = line.decode().split(',')\n"
        "\n"
        "        if header is None:\n"
        "            header = parts\n"
        "            print(f'  Header: {header}')\n"
        "        else:\n"
        "            record = dict(zip(header, parts))\n"
        "            records.append(record)\n"
        "            print(f'  Record: {record}')\n"
        "\n"
        "    return records\n"
    >>),

    {ok, Records} = py:eval(Ctx, <<"process_csv(buf)">>, #{<<"buf">> => Buf}),
    io:format("Parsed ~p records~n~n", [length(Records)]),
    ok.

asyncio_demo() ->
    io:format("--- Async I/O Demo (Erlang streaming to Python) ---~n~n"),

    %% Create buffer that Erlang will fill
    {ok, Buf} = py_buffer:new(),
    Self = self(),

    Ctx = py:context(1),

    ok = py:exec(Ctx, <<
        "\n"
        "import asyncio\n"
        "\n"
        "async def async_buffer_reader(buf):\n"
        "    '''Read from buffer asynchronously as Erlang streams data.'''\n"
        "    chunks = []\n"
        "    read_count = 0\n"
        "\n"
        "    while not buf.at_eof():\n"
        "        available = buf.readable_amount()\n"
        "        if available > 0:\n"
        "            chunk = buf.read_nonblock(available)\n"
        "            chunks.append(chunk)\n"
        "            read_count += 1\n"
        "            print(f'    [Python] Read chunk {read_count}: {len(chunk)} bytes')\n"
        "        else:\n"
        "            # Yield to event loop while waiting for Erlang to write more\n"
        "            await asyncio.sleep(0.005)\n"
        "\n"
        "    return b''.join(chunks)\n"
        "\n"
        "def run_async_reader(buf):\n"
        "    '''Run async reader.'''\n"
        "    loop = asyncio.new_event_loop()\n"
        "    try:\n"
        "        return loop.run_until_complete(async_buffer_reader(buf))\n"
        "    finally:\n"
        "        loop.close()\n"
    >>),

    %% Spawn a process to stream data from Erlang
    spawn_link(fun() ->
        Chunks = [
            <<"Hello from Erlang!">>,
            <<" Streaming chunk 2.">>,
            <<" Final chunk 3.">>
        ],
        lists:foreach(
            fun(Chunk) ->
                %% Simulate network delay
                timer:sleep(30),
                io:format("  [Erlang] Writing: ~p~n", [Chunk]),
                ok = py_buffer:write(Buf, Chunk)
            end,
            Chunks
        ),
        timer:sleep(10),
        io:format("  [Erlang] Closing buffer (EOF)~n"),
        ok = py_buffer:close(Buf),
        Self ! writer_done
    end),

    %% Python reads asynchronously while Erlang writes
    io:format("  Starting async read while Erlang streams...~n"),
    {ok, Result} = py:eval(Ctx, <<"run_async_reader(buf)">>, #{<<"buf">> => Buf}),

    %% Wait for writer
    receive
        writer_done -> ok
    after 2000 -> ok
    end,

    io:format("  Final result: ~p~n~n", [Result]),
    ok.
