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

-module(py_async_call_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(100),
    ok.

cleanup(_) ->
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

async_call_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"async_call returns ref", fun test_async_call_returns_ref/0},
      {"async_await returns result", fun test_async_await_returns_result/0},
      {"async_await handles error", fun test_async_await_handles_error/0},
      {"multiple async_calls", fun test_multiple_async_calls/0}
     ]}.

test_async_call_returns_ref() ->
    Ref = py:async_call(asyncio, sleep, [0.001]),
    ?assert(is_reference(Ref)).

test_async_await_returns_result() ->
    %% asyncio.sleep returns None
    Ref = py:async_call(asyncio, sleep, [0.001]),
    Result = py:async_await(Ref),
    ?assertEqual({ok, none}, Result).

test_async_await_handles_error() ->
    %% Call non-existent module
    Ref = py:async_call(nonexistent_module_xyz, some_func, []),
    Result = py:async_await(Ref, 5000),
    ?assertMatch({error, _}, Result).

test_multiple_async_calls() ->
    %% Submit multiple async calls
    Refs = [py:async_call(asyncio, sleep, [0.001 * N]) || N <- lists:seq(1, 5)],

    %% Wait for all
    Results = [py:async_await(Ref, 5000) || Ref <- Refs],

    %% All should succeed with none
    ?assertEqual(5, length(Results)),
    lists:foreach(fun(R) ->
        ?assertEqual({ok, none}, R)
    end, Results).
