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

-module(py_async_driver_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    %% Start application which initializes Python and async driver
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Give time for initialization
    timer:sleep(100),
    ok.

cleanup(_) ->
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

async_driver_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start/stop lifecycle", fun test_lifecycle/0},
      {"get_event_proc returns pid", fun test_get_event_proc/0},
      {"submit returns ref", fun test_submit_returns_ref/0},
      {"submit delivers result", fun test_submit_delivers_result/0},
      {"submit delivers error", fun test_submit_delivers_error/0},
      {"concurrent coroutines", fun test_concurrent_coroutines/0}
     ]}.

test_lifecycle() ->
    %% Driver should already be started by application
    {ok, Pid} = py_async_driver:get_event_proc(),
    ?assert(is_pid(Pid)).

test_get_event_proc() ->
    {ok, Pid} = py_async_driver:get_event_proc(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

test_submit_returns_ref() ->
    %% Submit a simple async function (using asyncio.sleep as a coroutine)
    Result = py_async_driver:submit(
        <<"asyncio">>,
        <<"sleep">>,
        [0.001],  %% 1ms sleep
        #{}
    ),
    ?assertMatch({ok, Ref} when is_reference(Ref), Result).

test_submit_delivers_result() ->
    %% Use asyncio.sleep which returns None after waiting
    %% This is a simple built-in coroutine
    {ok, Ref} = py_async_driver:submit(
        <<"asyncio">>,
        <<"sleep">>,
        [0.001],  %% 1ms sleep
        #{}
    ),

    receive
        {py_result, Ref, Result} ->
            %% asyncio.sleep returns None
            ?assertEqual(none, Result)
    after 5000 ->
        ?assert(false)
    end.

test_submit_delivers_error() ->
    %% Submit a call to a non-existent module
    {ok, Ref} = py_async_driver:submit(
        <<"nonexistent_module_xyz">>,
        <<"some_func">>,
        [],
        #{}
    ),

    receive
        {py_error, Ref, _Error} ->
            ok
    after 5000 ->
        ?assert(false)
    end.

test_concurrent_coroutines() ->
    NumCoroutines = 10,

    %% Submit multiple asyncio.sleep coroutines concurrently
    Refs = lists:map(fun(N) ->
        {ok, Ref} = py_async_driver:submit(
            <<"asyncio">>,
            <<"sleep">>,
            [0.001 * N],  %% Varying sleep times
            #{}
        ),
        {N, Ref}
    end, lists:seq(1, NumCoroutines)),

    %% Collect all results
    Results = lists:map(fun({N, Ref}) ->
        receive
            {py_result, Ref, _Result} ->
                N
        after 10000 ->
            error({timeout, N})
        end
    end, Refs),

    %% Verify we got all results
    ?assertEqual(NumCoroutines, length(Results)).
