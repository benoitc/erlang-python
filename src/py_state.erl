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

%%% @doc Shared state storage for Python workers.
%%%
%%% This module provides a simple key-value store backed by ETS that
%%% Python code can use to share state between workers. Since each
%%% Python worker has its own namespace, this provides a way to share
%%% data across calls.
%%%
%%% == Python Usage ==
%%% ```python
%%% from erlang import state_set, state_get, state_delete, state_keys
%%% from erlang import state_incr, state_decr
%%%
%%% # Store data
%%% state_set('my_key', {'data': [1, 2, 3]})
%%%
%%% # Retrieve data
%%% value = state_get('my_key')  # {'data': [1, 2, 3]}
%%%
%%% # Atomic counters (thread-safe)
%%% state_incr('hits')        # increment by 1, returns new value
%%% state_incr('hits', 10)    # increment by 10
%%% state_decr('hits')        # decrement by 1
%%% state_decr('hits', 5)     # decrement by 5
%%%
%%% # Delete data
%%% state_delete('my_key')
%%%
%%% # List all keys
%%% keys = state_keys()  # ['other_key', ...]
%%% '''
%%%
%%% == Erlang Usage ==
%%% ```erlang
%%% py_state:store(<<"my_key">>, #{data => [1, 2, 3]}).
%%% {ok, Value} = py_state:fetch(<<"my_key">>).
%%%
%%% %% Atomic counters
%%% 1 = py_state:incr(<<"counter">>).
%%% 11 = py_state:incr(<<"counter">>, 10).
%%% 10 = py_state:decr(<<"counter">>).
%%% '''
%%% @end
-module(py_state).

-export([
    init_tab/0,
    register_callbacks/0,
    fetch/1,
    store/2,
    remove/1,
    keys/0,
    clear/0,
    incr/1,
    incr/2,
    decr/1,
    decr/2
]).

-define(TABLE, py_state).

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Initialize the ETS table for shared state.
%% Called by supervisor for resilience - table survives process crashes.
-spec init_tab() -> ok.
init_tab() ->
    ?TABLE = ets:new(?TABLE, [
        named_table,
        public,
        set,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ok.

%% @doc Register state functions as callbacks for Python access.
%% Called after py_callback is started.
-spec register_callbacks() -> ok.
register_callbacks() ->
    py_callback:register(state_get, fun state_get_callback/1),
    py_callback:register(state_set, fun state_set_callback/1),
    py_callback:register(state_delete, fun state_delete_callback/1),
    py_callback:register(state_keys, fun state_keys_callback/1),
    py_callback:register(state_clear, fun state_clear_callback/1),
    py_callback:register(state_incr, fun state_incr_callback/1),
    py_callback:register(state_decr, fun state_decr_callback/1),
    %% Internal callback for stream_start to fetch stored values
    py_callback:register(<<"_py_state_fetch">>, fun state_fetch_internal/1),
    %% Check if stream is cancelled
    py_callback:register(<<"_py_stream_cancelled">>, fun stream_cancelled_callback/1),
    %% Send stream event to owner
    py_callback:register(<<"_py_stream_send">>, fun stream_send_callback/1),
    %% Clean up stream state
    py_callback:register(<<"_py_stream_cleanup">>, fun stream_cleanup_callback/1),
    ok.

%% @doc Fetch a value from the shared state.
-spec fetch(Key :: term()) -> {ok, term()} | {error, not_found}.
fetch(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{_, Value}] -> {ok, Value};
        [] -> {error, not_found}
    end.

%% @doc Store a value in the shared state.
-spec store(Key :: term(), Value :: term()) -> ok.
store(Key, Value) ->
    ets:insert(?TABLE, {Key, Value}),
    ok.

%% @doc Remove a key from the shared state.
-spec remove(Key :: term()) -> ok.
remove(Key) ->
    ets:delete(?TABLE, Key),
    ok.

%% @doc Get all keys in the shared state.
-spec keys() -> [term()].
keys() ->
    ets:foldl(fun({K, _}, Acc) -> [K | Acc] end, [], ?TABLE).

%% @doc Clear all entries from the shared state.
-spec clear() -> ok.
clear() ->
    ets:delete_all_objects(?TABLE),
    ok.

%% @doc Atomically increment a counter by 1. Initializes to 1 if not exists.
-spec incr(Key :: term()) -> integer().
incr(Key) ->
    incr(Key, 1).

%% @doc Atomically increment a counter by Amount. Initializes to Amount if not exists.
-spec incr(Key :: term(), Amount :: integer()) -> integer().
incr(Key, Amount) ->
    try
        ets:update_counter(?TABLE, Key, {2, Amount})
    catch
        error:badarg ->
            %% Key doesn't exist, initialize it
            ets:insert_new(?TABLE, {Key, Amount}),
            Amount
    end.

%% @doc Atomically decrement a counter by 1.
-spec decr(Key :: term()) -> integer().
decr(Key) ->
    incr(Key, -1).

%% @doc Atomically decrement a counter by Amount.
-spec decr(Key :: term(), Amount :: integer()) -> integer().
decr(Key, Amount) ->
    incr(Key, -Amount).

%%% ============================================================================
%%% Callback wrappers (for Python access)
%%% ============================================================================

%% @private
state_get_callback([Key]) ->
    case fetch(Key) of
        {ok, Value} -> Value;
        {error, not_found} -> none
    end.

%% @private
state_set_callback([Key, Value]) ->
    store(Key, Value),
    none.

%% @private
state_delete_callback([Key]) ->
    remove(Key),
    none.

%% @private
state_keys_callback([]) ->
    keys();
state_keys_callback(_) ->
    keys().

%% @private
state_clear_callback([]) ->
    clear(),
    none;
state_clear_callback(_) ->
    clear(),
    none.

%% @private
state_incr_callback([Key]) ->
    incr(Key);
state_incr_callback([Key, Amount]) ->
    incr(Key, Amount).

%% @private
state_decr_callback([Key]) ->
    decr(Key);
state_decr_callback([Key, Amount]) ->
    decr(Key, Amount).

%% @private Internal fetch for stream_start to pass args/pid/ref to Python
state_fetch_internal([{Type, Key}]) ->
    case fetch({Type, Key}) of
        {ok, Value} ->
            %% Clean up after fetching (one-time use)
            remove({Type, Key}),
            Value;
        {error, not_found} ->
            none
    end;
state_fetch_internal([Type, Key]) ->
    state_fetch_internal([{Type, Key}]).

%% @private Check if a stream has been cancelled
stream_cancelled_callback([RefHash]) ->
    %% Use binary key because Python strings become binaries
    case fetch({<<"stream_cancelled_hash">>, RefHash}) of
        {ok, true} ->
            %% Clean up the cancellation flag
            remove({<<"stream_cancelled_hash">>, RefHash}),
            true;
        {error, not_found} ->
            false
    end.

%% @private Send a stream event to the owner process
%% Called from Python as erlang.call('_py_stream_send', [RefHash, EventType, Value])
%% EventType: 'data' | 'done' | 'error' (may come as binary from Python)
stream_send_callback([RefHash, EventType, Value]) ->
    %% Use binary keys because Python strings become binaries
    case fetch({<<"stream_owner">>, RefHash}) of
        {ok, Owner} ->
            case fetch({<<"stream_ref">>, RefHash}) of
                {ok, Ref} ->
                    Event = case normalize_event_type(EventType) of
                        done -> done;
                        data -> {data, Value};
                        error -> {error, Value};
                        Other -> {error, {unknown_event, Other, Value}}
                    end,
                    Owner ! {py_stream, Ref, Event},
                    ok;
                {error, not_found} ->
                    {error, ref_not_found}
            end;
        {error, not_found} ->
            {error, owner_not_found}
    end.

%% @private Normalize event type from Python (may come as binary or atom)
normalize_event_type(done) -> done;
normalize_event_type(data) -> data;
normalize_event_type(error) -> error;
normalize_event_type(<<"done">>) -> done;
normalize_event_type(<<"data">>) -> data;
normalize_event_type(<<"error">>) -> error;
normalize_event_type(Other) -> Other.

%% @private Clean up stream state entries
stream_cleanup_callback([RefHash]) ->
    remove({<<"stream_owner">>, RefHash}),
    remove({<<"stream_ref">>, RefHash}),
    remove({<<"stream_args">>, RefHash}),
    ok.
