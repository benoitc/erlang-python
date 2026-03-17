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

%%% @doc Raw byte channel for Erlang-Python communication.
%%%
%%% ByteChannel provides raw byte streaming without term serialization,
%%% suitable for HTTP bodies, file transfers, and binary protocols.
%%%
%%% Unlike py_channel which serializes Erlang terms, ByteChannel passes
%%% raw binaries directly without any encoding/decoding overhead.
%%%
%%% == Usage ==
%%%
%%% ```
%%% %% Create a byte channel
%%% {ok, Ch} = py_byte_channel:new(),
%%%
%%% %% Send raw bytes to Python
%%% ok = py_byte_channel:send(Ch, <<"HTTP/1.1 200 OK\r\n">>),
%%%
%%% %% Python receives via ByteChannel.receive_bytes()
%%% %% Python sends back via ByteChannel.send_bytes()
%%%
%%% %% Close when done
%%% py_byte_channel:close(Ch).
%%% '''
%%%
%%% == When to Use ==
%%%
%%% <ul>
%%% <li>HTTP request/response bodies</li>
%%% <li>File streaming</li>
%%% <li>Binary protocols without Erlang term overhead</li>
%%% </ul>
%%%
%%% @end
-module(py_byte_channel).

-export([
    new/0,
    new/1,
    send/2,
    try_receive/1,
    recv/1,
    recv/2,
    close/1,
    info/1,
    %% Callback handlers for Python
    register_callbacks/0
]).

%% Internal callback handlers
-export([
    handle_receive_bytes/1,
    handle_try_receive_bytes/1,
    handle_wait_bytes/1,
    handle_cancel_wait_bytes/1
]).

-type channel() :: reference().
-type opts() :: #{
    max_size => non_neg_integer()
}.

-export_type([channel/0, opts/0]).

%% @doc Create a new byte channel with default settings.
%%
%% Creates an unbounded channel for raw byte passing.
%%
%% @returns {ok, Channel} | {error, Reason}
-spec new() -> {ok, channel()} | {error, term()}.
new() ->
    new(#{}).

%% @doc Create a new byte channel with options.
%%
%% Options:
%% <ul>
%% <li>`max_size' - Maximum queue size in bytes for backpressure (0 = unlimited)</li>
%% </ul>
%%
%% @param Opts Channel options
%% @returns {ok, Channel} | {error, Reason}
-spec new(opts()) -> {ok, channel()} | {error, term()}.
new(Opts) when is_map(Opts) ->
    MaxSize = maps:get(max_size, Opts, 0),
    py_nif:channel_create(MaxSize).

%% @doc Send raw bytes to a channel.
%%
%% The binary is queued directly for Python to receive without
%% any term serialization.
%% If the queue exceeds max_size, returns `busy' (backpressure).
%%
%% @param Channel Channel reference
%% @param Bytes Binary data to send
%% @returns ok | busy | {error, closed}
-spec send(channel(), binary()) -> ok | busy | {error, term()}.
send(Channel, Bytes) when is_binary(Bytes) ->
    py_nif:byte_channel_send_bytes(Channel, Bytes).

%% @doc Try to receive raw bytes from a channel (non-blocking).
%%
%% Returns immediately with binary data or empty/closed status.
%%
%% @param Channel Channel reference
%% @returns {ok, Binary} | {error, empty} | {error, closed}
-spec try_receive(channel()) -> {ok, binary()} | {error, empty | closed | term()}.
try_receive(Channel) ->
    py_nif:byte_channel_try_receive_bytes(Channel).

%% @doc Receive raw bytes from a channel (blocking).
%%
%% Blocks until data is available. Equivalent to recv(Channel, infinity).
%%
%% @param Channel Channel reference
%% @returns {ok, Binary} | {error, closed}
-spec recv(channel()) -> {ok, binary()} | {error, closed | term()}.
recv(Channel) ->
    ?MODULE:recv(Channel, infinity).

%% @doc Receive raw bytes from a channel with timeout.
%%
%% Blocks until data is available or timeout expires.
%%
%% @param Channel Channel reference
%% @param Timeout Timeout in milliseconds or 'infinity'
%% @returns {ok, Binary} | {error, closed} | {error, timeout}
-spec recv(channel(), timeout()) -> {ok, binary()} | {error, closed | timeout | term()}.
recv(Channel, Timeout) ->
    case try_receive(Channel) of
        {ok, Data} ->
            {ok, Data};
        {error, closed} ->
            {error, closed};
        {error, empty} ->
            case py_nif:channel_register_sync_waiter(Channel) of
                ok ->
                    wait_for_bytes(Channel, Timeout);
                has_data ->
                    %% Race condition: data arrived, retry
                    ?MODULE:recv(Channel, Timeout);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
%% Wait for bytes to arrive via Erlang message passing.
-spec wait_for_bytes(channel(), timeout()) -> {ok, binary()} | {error, term()}.
wait_for_bytes(Channel, Timeout) ->
    erlang_receive(Channel, Timeout).

%% @private
%% Internal receive that uses the erlang receive keyword
erlang_receive(Channel, Timeout) ->
    receive
        channel_data_ready ->
            case try_receive(Channel) of
                {ok, Data} ->
                    {ok, Data};
                {error, empty} ->
                    %% Race: data was consumed, re-register and wait
                    case py_nif:channel_register_sync_waiter(Channel) of
                        ok ->
                            erlang_receive(Channel, Timeout);
                        has_data ->
                            ?MODULE:recv(Channel, Timeout);
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, closed} ->
                    {error, closed}
            end;
        channel_closed ->
            {error, closed}
    after Timeout ->
        {error, timeout}
    end.

%% @doc Close a byte channel.
%%
%% Signals Python receivers that no more bytes will arrive.
%%
%% @param Channel Channel reference
%% @returns ok
-spec close(channel()) -> ok.
close(Channel) ->
    py_nif:channel_close(Channel).

%% @doc Get channel information.
%%
%% Returns a map with:
%% <ul>
%% <li>`size' - Current queue size in bytes</li>
%% <li>`max_size' - Maximum queue size (0 = unlimited)</li>
%% <li>`closed' - Whether the channel is closed</li>
%% </ul>
%%
%% @param Channel Channel reference
%% @returns Info map
-spec info(channel()) -> map().
info(Channel) ->
    py_nif:channel_info(Channel).

%%% ============================================================================
%%% Python Callback Registration
%%% ============================================================================

%% @doc Register byte channel callback handlers for Python.
%%
%% This should be called during application startup to enable
%% Python's erlang.call('_py_byte_channel_receive', ...) etc.
-spec register_callbacks() -> ok.
register_callbacks() ->
    py_callback:register(<<"_py_byte_channel_receive">>, {?MODULE, handle_receive_bytes}),
    py_callback:register(<<"_py_byte_channel_try_receive">>, {?MODULE, handle_try_receive_bytes}),
    py_callback:register(<<"_py_byte_channel_wait">>, {?MODULE, handle_wait_bytes}),
    py_callback:register(<<"_py_byte_channel_cancel_wait">>, {?MODULE, handle_cancel_wait_bytes}),
    ok.

%% @private
%% Handle blocking receive from Python.
%% This blocks until data is available by registering as a sync waiter.
%% Args: [ChannelRef]
-spec handle_receive_bytes([term()]) -> term().
handle_receive_bytes([ChannelRef]) ->
    case try_receive(ChannelRef) of
        {ok, Data} ->
            {ok, Data};
        {error, closed} ->
            {error, closed};
        {error, empty} ->
            case py_nif:channel_register_sync_waiter(ChannelRef) of
                ok ->
                    wait_for_bytes_callback(ChannelRef);
                has_data ->
                    handle_receive_bytes([ChannelRef]);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
%% Wait for bytes in callback context (no timeout).
-spec wait_for_bytes_callback(reference()) -> {ok, binary()} | {error, term()}.
wait_for_bytes_callback(ChannelRef) ->
    receive
        channel_data_ready ->
            case try_receive(ChannelRef) of
                {ok, Data} ->
                    {ok, Data};
                {error, empty} ->
                    case py_nif:channel_register_sync_waiter(ChannelRef) of
                        ok ->
                            wait_for_bytes_callback(ChannelRef);
                        has_data ->
                            handle_receive_bytes([ChannelRef]);
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, closed} ->
                    {error, closed}
            end;
        channel_closed ->
            {error, closed}
    end.

%% @private
%% Handle non-blocking receive from Python.
%% Args: [ChannelRef]
-spec handle_try_receive_bytes([term()]) -> term().
handle_try_receive_bytes([ChannelRef]) ->
    try_receive(ChannelRef).

%% @private
%% Handle async wait registration from Python.
%% Args: [ChannelRef, CallbackId, LoopRef]
-spec handle_wait_bytes([term()]) -> ok | {ok, binary()} | {error, term()}.
handle_wait_bytes([ChannelRef, CallbackId, LoopRef]) ->
    py_nif:byte_channel_wait_bytes(ChannelRef, CallbackId, LoopRef).

%% @private
%% Handle cancel wait from Python.
%% Args: [ChannelRef, CallbackId]
-spec handle_cancel_wait_bytes([term()]) -> ok.
handle_cancel_wait_bytes([ChannelRef, CallbackId]) ->
    py_nif:channel_cancel_wait(ChannelRef, CallbackId).
