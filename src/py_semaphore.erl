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

%%%-------------------------------------------------------------------
%%% @doc ETS-based counting semaphore for rate limiting Python operations.
%%%
%%% Based on the Discord semaphore pattern. Uses atomic ETS operations
%%% for high concurrency without a gen_server bottleneck.
%%%
%%% The semaphore limits concurrent Python operations to prevent:
%%% - Memory exhaustion from unbounded request queuing
%%% - Dirty scheduler pool starvation
%%% - System overload under burst traffic
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(py_semaphore).

-export([
    init/0,
    acquire/1,
    release/0,
    max_concurrent/0,
    current/0,
    set_max_concurrent/1
]).

-define(TABLE, py_semaphore).
-define(COUNTER_KEY, running).
-define(MAX_KEY, max).
-define(BACKOFF_MS, 5).
-define(MAX_BACKOFF_MS, 50).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the semaphore ETS table.
%% Safe to call multiple times - will not recreate if already exists.
-spec init() -> ok.
init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            %% Create table with write_concurrency for atomic counter operations
            _ = ets:new(?TABLE, [
                named_table,
                public,
                {write_concurrency, true},
                {read_concurrency, true}
            ]),
            Max = default_max_concurrent(),
            ets:insert(?TABLE, [{?COUNTER_KEY, 0}, {?MAX_KEY, Max}]),
            ok;
        _Tid ->
            ok
    end.

%% @doc Acquire a slot in the semaphore.
%% Blocks with exponential backoff until a slot is available or timeout.
%% Returns ok on success, {error, max_concurrent} on timeout.
-spec acquire(timeout()) -> ok | {error, max_concurrent}.
acquire(infinity) ->
    acquire_loop(infinity, 0, ?BACKOFF_MS);
acquire(Timeout) when is_integer(Timeout), Timeout > 0 ->
    StartTime = erlang:monotonic_time(millisecond),
    acquire_loop(Timeout, StartTime, ?BACKOFF_MS);
acquire(_) ->
    acquire(30000). % Default 30s timeout

%% @doc Release a slot in the semaphore.
%% Must be called after acquire/1 completes, typically in an after clause.
-spec release() -> ok.
release() ->
    %% Atomically decrement, ensuring we don't go below 0
    %% {Pos, Increment, Threshold, SetValue}
    %% Decrement position 2 by 1, if result < 0, set to 0
    _ = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, -1, 0, 0}),
    ok.

%% @doc Get the maximum concurrent operations allowed.
-spec max_concurrent() -> pos_integer().
max_concurrent() ->
    case ets:lookup(?TABLE, ?MAX_KEY) of
        [{?MAX_KEY, Max}] -> Max;
        [] -> default_max_concurrent()
    end.

%% @doc Get the current number of operations in flight.
-spec current() -> non_neg_integer().
current() ->
    case ets:lookup(?TABLE, ?COUNTER_KEY) of
        [{?COUNTER_KEY, Count}] -> Count;
        [] -> 0
    end.

%% @doc Dynamically set the maximum concurrent operations.
%% Takes effect immediately for new acquire calls.
-spec set_max_concurrent(pos_integer()) -> ok.
set_max_concurrent(Max) when is_integer(Max), Max > 0 ->
    ets:insert(?TABLE, {?MAX_KEY, Max}),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire_loop(timeout(), non_neg_integer(), pos_integer()) ->
    ok | {error, max_concurrent}.
acquire_loop(Timeout, StartTime, Backoff) ->
    Max = max_concurrent(),
    %% Atomically increment and get new value
    N = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, 1}),
    if
        N =< Max ->
            %% Successfully acquired slot
            ok;
        true ->
            %% Over limit - decrement back and wait
            _ = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, -1}),
            case check_timeout(Timeout, StartTime) of
                continue ->
                    %% Exponential backoff with jitter
                    Jitter = rand:uniform(Backoff div 2 + 1),
                    SleepTime = Backoff + Jitter,
                    timer:sleep(SleepTime),
                    NewBackoff = min(Backoff * 2, ?MAX_BACKOFF_MS),
                    acquire_loop(Timeout, StartTime, NewBackoff);
                timeout ->
                    {error, max_concurrent}
            end
    end.

-spec check_timeout(timeout(), non_neg_integer()) -> continue | timeout.
check_timeout(infinity, _StartTime) ->
    continue;
check_timeout(Timeout, StartTime) ->
    Elapsed = erlang:monotonic_time(millisecond) - StartTime,
    case Elapsed >= Timeout of
        true -> timeout;
        false -> continue
    end.

-spec default_max_concurrent() -> pos_integer().
default_max_concurrent() ->
    case application:get_env(erlang_python, max_concurrent) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> erlang:system_info(schedulers) * 2 + 1
    end.
