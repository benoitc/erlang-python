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

%%% @doc Atomic callback ID generator for event-driven operations.
%%%
%%% Provides unique, monotonically increasing callback IDs used to correlate
%%% async operations with their results. Uses atomics for lock-free,
%%% thread-safe ID generation.
%%%
%%% @private
-module(py_callback_id).

-export([init/0, next/0]).

-define(COUNTER_KEY, py_callback_id_counter).

%% @doc Initialize the callback ID counter.
%% Must be called once during application startup.
%% Uses persistent_term for fast read access.
-spec init() -> ok.
init() ->
    Counter = atomics:new(1, [{signed, false}]),
    persistent_term:put(?COUNTER_KEY, Counter),
    ok.

%% @doc Get the next unique callback ID.
%% Thread-safe, lock-free, monotonically increasing.
%% Returns a positive integer starting from 1.
-spec next() -> pos_integer().
next() ->
    Counter = persistent_term:get(?COUNTER_KEY),
    atomics:add_get(Counter, 1, 1).
