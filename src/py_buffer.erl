%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Zero-copy streaming buffer for transferring data from Erlang to Python.
%%%
%%% This module provides a buffer that can be written by Erlang and read
%%% by Python with zero-copy semantics. The buffer supports blocking reads
%%% that release the GIL while waiting for data.
%%%
%%% == Usage ==
%%%
%%% ```
%%% %% Create a buffer (streaming - unknown size)
%%% {ok, Buf} = py_buffer:new(),
%%%
%%% %% Or with known content length
%%% {ok, Buf} = py_buffer:new(1024),
%%%
%%% %% Write data chunks
%%% ok = py_buffer:write(Buf, <<"chunk1">>),
%%% ok = py_buffer:write(Buf, <<"chunk2">>),
%%%
%%% %% Signal end of data
%%% ok = py_buffer:close(Buf),
%%%
%%% %% Pass to Python handler
%%% py_context:call(Ctx, <<"myapp">>, <<"handle">>, [Buf], #{}).
%%% '''
%%%
%%% On the Python side, the buffer provides a file-like interface:
%%%
%%% ```python
%%% def handle(buf):
%%%     body = buf.read()  # Blocks until data ready
%%%     # Or use readline(), readlines(), iteration
%%%     for line in buf:
%%%         process(line)
%%% '''
%%%
%%% @end
-module(py_buffer).

-export([
    new/0,
    new/1,
    write/2,
    close/1
]).

%% @doc Create a new buffer for chunked/streaming data.
%%
%% Use this when the content length is unknown (chunked transfer encoding).
%% The buffer will grow as needed.
%%
%% @returns {ok, BufferRef} | {error, Reason}
-spec new() -> {ok, reference()} | {error, term()}.
new() ->
    py_nif:py_buffer_create(undefined).

%% @doc Create a new buffer with known content length.
%%
%% Pre-allocates the buffer to the specified size for better performance.
%%
%% @param ContentLength Expected total size in bytes, or `undefined' for chunked
%% @returns {ok, BufferRef} | {error, Reason}
-spec new(non_neg_integer() | undefined) -> {ok, reference()} | {error, term()}.
new(undefined) ->
    py_nif:py_buffer_create(undefined);
new(ContentLength) when is_integer(ContentLength), ContentLength >= 0 ->
    py_nif:py_buffer_create(ContentLength).

%% @doc Write data to the buffer.
%%
%% Appends data to the buffer and signals any waiting Python readers.
%% This function is safe to call from multiple processes, but typically
%% only one process should write to a buffer.
%%
%% @param Ref Buffer reference from new/0 or new/1
%% @param Data Binary data to append
%% @returns ok | {error, Reason}
-spec write(reference(), binary()) -> ok | {error, term()}.
write(Ref, Data) when is_binary(Data) ->
    py_nif:py_buffer_write(Ref, Data).

%% @doc Close the buffer (signal end of data).
%%
%% Sets the EOF flag and wakes up any Python threads waiting for data.
%% After calling close, no more data can be written, and Python's read()
%% will return any remaining buffered data followed by empty bytes.
%%
%% @param Ref Buffer reference
%% @returns ok
-spec close(reference()) -> ok.
close(Ref) ->
    py_nif:py_buffer_close(Ref).
