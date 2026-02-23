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

-module(py_call_event_driven_test).

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

event_driven_call_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"basic call works", fun test_basic_call/0},
      {"call with kwargs", fun test_call_with_kwargs/0},
      {"call error handling", fun test_call_error/0},
      {"concurrent calls", fun test_concurrent_calls/0},
      {"bound calls use py_pool", fun test_bound_calls/0}
     ]}.

test_basic_call() ->
    %% Simple math call
    Result = py:call(math, sqrt, [16.0]),
    ?assertEqual({ok, 4.0}, Result).

test_call_with_kwargs() ->
    %% Call with keyword arguments
    Result = py:call(json, dumps, [[1, 2, 3]], #{indent => 2}),
    ?assertMatch({ok, _}, Result).

test_call_error() ->
    %% Call non-existent module
    Result = py:call(nonexistent_module_xyz, some_func, []),
    ?assertMatch({error, _}, Result).

test_concurrent_calls() ->
    %% Submit multiple concurrent calls
    Self = self(),
    NumCalls = 10,

    %% Spawn processes to make concurrent calls
    Pids = [spawn_link(fun() ->
        Result = py:call(math, pow, [float(N), 2.0]),
        Self ! {done, N, Result}
    end) || N <- lists:seq(1, NumCalls)],

    %% Collect results
    Results = [receive {done, N, R} -> {N, R} after 10000 -> error({timeout, N}) end
               || N <- lists:seq(1, NumCalls)],

    %% Verify all succeeded with correct values
    ?assertEqual(NumCalls, length(Results)),
    lists:foreach(fun({N, {ok, R}}) ->
        Expected = float(N * N),
        ?assert(abs(R - Expected) < 0.001)
    end, Results),

    %% Wait for spawned processes to exit
    [receive {'EXIT', Pid, _} -> ok after 100 -> ok end || Pid <- Pids].

test_bound_calls() ->
    %% Bound processes should still use py_pool and preserve state
    ok = py:bind(),
    try
        ok = py:exec(<<"test_var = 42">>),
        {ok, 42} = py:eval(<<"test_var">>),
        %% Call should also work in bound context
        {ok, 4.0} = py:call(math, sqrt, [16.0])
    after
        py:unbind()
    end.
