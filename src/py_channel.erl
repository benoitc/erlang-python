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

%%% @doc Bidirectional channel for Erlang-Python communication.
%%%
%%% Channels provide efficient streaming message passing between Erlang
%%% processes and Python code without syscall overhead.
%%%
%%% == Usage ==
%%%
%%% ```
%%% %% Create a channel
%%% {ok, Ch} = py_channel:new(),
%%%
%%% %% Send messages to Python
%%% ok = py_channel:send(Ch, {request, self(), <<"data">>}),
%%%
%%% %% Python receives via channel.receive()
%%% %% Python sends back via erlang.channel_reply(pid, term)
%%%
%%% %% Close when done
%%% py_channel:close(Ch).
%%% '''
%%%
%%% == Backpressure ==
%%%
%%% Channels support backpressure via max_size option:
%%%
%%% ```
%%% {ok, Ch} = py_channel:new(#{max_size => 10000}),
%%% %% Returns 'busy' when queue exceeds max_size
%%% case py_channel:send(Ch, LargeData) of
%%%     ok -> proceed;
%%%     busy -> wait_and_retry
%%% end.
%%% '''
%%%
%%% @end
-module(py_channel).

-export([
    new/0,
    new/1,
    send/2,
    close/1,
    info/1,
    %% Callback handlers for Python
    register_callbacks/0
]).

%% Internal callback handlers
-export([
    handle_receive/1,
    handle_try_receive/1,
    handle_wait/1,
    handle_cancel_wait/1,
    handle_info/1
]).

-type channel() :: reference().
-type opts() :: #{
    max_size => non_neg_integer()
}.

-export_type([channel/0, opts/0]).

%% @doc Create a new channel with default settings.
%%
%% Creates an unbounded channel for message passing.
%%
%% @returns {ok, Channel} | {error, Reason}
-spec new() -> {ok, channel()} | {error, term()}.
new() ->
    new(#{}).

%% @doc Create a new channel with options.
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

%% @doc Send a message to a channel.
%%
%% The term is serialized and queued for Python to receive.
%% If the queue exceeds max_size, returns `busy' (backpressure).
%%
%% @param Channel Channel reference
%% @param Term Erlang term to send
%% @returns ok | busy | {error, closed}
-spec send(channel(), term()) -> ok | busy | {error, term()}.
send(Channel, Term) ->
    py_nif:channel_send(Channel, Term).

%% @doc Close a channel.
%%
%% Signals Python receivers that no more messages will arrive.
%% Any blocked receive() calls will raise StopIteration.
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

%% @doc Register channel callback handlers for Python.
%%
%% This should be called during application startup to enable
%% Python's erlang.call('_py_channel_receive', ...) etc.
-spec register_callbacks() -> ok.
register_callbacks() ->
    py_callback:register(<<"_py_channel_receive">>, {?MODULE, handle_receive}),
    py_callback:register(<<"_py_channel_try_receive">>, {?MODULE, handle_try_receive}),
    py_callback:register(<<"_py_channel_wait">>, {?MODULE, handle_wait}),
    py_callback:register(<<"_py_channel_cancel_wait">>, {?MODULE, handle_cancel_wait}),
    py_callback:register(<<"_py_channel_info">>, {?MODULE, handle_info}),
    ok.

%% @private
%% Handle blocking receive from Python.
%% Args: [ChannelRef]
-spec handle_receive([term()]) -> term().
handle_receive([ChannelRef]) ->
    py_nif:channel_try_receive(ChannelRef).

%% @private
%% Handle non-blocking receive from Python.
%% Args: [ChannelRef]
-spec handle_try_receive([term()]) -> term().
handle_try_receive([ChannelRef]) ->
    py_nif:channel_try_receive(ChannelRef).

%% @private
%% Handle async wait registration from Python.
%% Args: [ChannelRef, CallbackId, LoopRef]
%% Returns ok if waiter registered, {ok, Data} if data was available immediately
-spec handle_wait([term()]) -> ok | {ok, term()} | {error, term()}.
handle_wait([ChannelRef, CallbackId, LoopRef]) ->
    py_nif:channel_wait(ChannelRef, CallbackId, LoopRef).

%% @private
%% Handle cancel wait from Python.
%% Args: [ChannelRef, CallbackId]
-spec handle_cancel_wait([term()]) -> ok.
handle_cancel_wait([ChannelRef, CallbackId]) ->
    py_nif:channel_cancel_wait(ChannelRef, CallbackId).

%% @private
%% Handle channel info request from Python.
%% Args: [ChannelRef]
-spec handle_info([term()]) -> map().
handle_info([ChannelRef]) ->
    py_nif:channel_info(ChannelRef).
