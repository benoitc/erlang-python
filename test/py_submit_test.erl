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

-module(py_submit_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    %% Start application which initializes Python and submit queue
    {ok, _} = application:ensure_all_started(erlang_python),
    %% Give time for initialization
    timer:sleep(100),
    %% Start event loop process for receiving results
    LoopRef = make_ref(),
    {ok, EventProcPid} = py_event_loop_proc:start_link(LoopRef),
    {EventProcPid, LoopRef}.

cleanup({EventProcPid, _LoopRef}) ->
    py_event_loop_proc:stop(EventProcPid),
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

submit_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun({EventProcPid, _LoopRef}) ->
         [
          {"submit_call returns ok", fun() -> test_submit_call_returns_ok(EventProcPid) end},
          {"submit_call delivers result", fun() -> test_submit_call_delivers_result(EventProcPid) end},
          {"submit_call delivers error", fun() -> test_submit_call_delivers_error(EventProcPid) end},
          {"multiple concurrent submits", fun() -> test_concurrent_submits(EventProcPid) end}
         ]
     end}.

test_submit_call_returns_ok(EventProcPid) ->
    CallbackId = py_callback_id:next(),
    Ref = make_ref(),

    %% Register to receive the result
    ok = py_event_loop_proc:register_call(EventProcPid, CallbackId, Ref),

    %% Submit a simple call
    Result = py_nif:submit_call(
        EventProcPid,
        CallbackId,
        <<"math">>,
        <<"sqrt">>,
        [4.0],
        #{}
    ),

    ?assertEqual(ok, Result).

test_submit_call_delivers_result(EventProcPid) ->
    CallbackId = py_callback_id:next(),
    Ref = make_ref(),

    %% Register to receive the result
    ok = py_event_loop_proc:register_call(EventProcPid, CallbackId, Ref),

    %% Submit a simple call
    ok = py_nif:submit_call(
        EventProcPid,
        CallbackId,
        <<"math">>,
        <<"sqrt">>,
        [16.0],
        #{}
    ),

    %% Wait for result
    receive
        {py_result, Ref, Result} ->
            ?assertEqual(4.0, Result)
    after 5000 ->
        ?assert(false)
    end.

test_submit_call_delivers_error(EventProcPid) ->
    CallbackId = py_callback_id:next(),
    Ref = make_ref(),

    %% Register to receive the result
    ok = py_event_loop_proc:register_call(EventProcPid, CallbackId, Ref),

    %% Submit a call that will error (non-existent module)
    ok = py_nif:submit_call(
        EventProcPid,
        CallbackId,
        <<"nonexistent_module_xyz">>,
        <<"some_func">>,
        [],
        #{}
    ),

    %% Wait for error
    receive
        {py_error, Ref, _Error} ->
            ok
    after 5000 ->
        ?assert(false)
    end.

test_concurrent_submits(EventProcPid) ->
    NumCalls = 20,

    %% Generate callback IDs and refs
    Calls = lists:map(fun(_) ->
        CallbackId = py_callback_id:next(),
        Ref = make_ref(),
        ok = py_event_loop_proc:register_call(EventProcPid, CallbackId, Ref),
        {CallbackId, Ref}
    end, lists:seq(1, NumCalls)),

    %% Submit all calls concurrently (calculate squares)
    lists:foreach(fun({CallbackId, _Ref}) ->
        N = CallbackId,  %% Use callback ID as input
        ok = py_nif:submit_call(
            EventProcPid,
            CallbackId,
            <<"math">>,
            <<"pow">>,
            [float(N), 2.0],
            #{}
        )
    end, Calls),

    %% Collect all results
    Results = lists:map(fun({CallbackId, Ref}) ->
        receive
            {py_result, Ref, Result} ->
                {CallbackId, Result}
        after 10000 ->
            error({timeout, CallbackId})
        end
    end, Calls),

    %% Verify we got all results
    ?assertEqual(NumCalls, length(Results)),

    %% Verify results are squares (approximately, due to floating point)
    lists:foreach(fun({CallbackId, Result}) ->
        Expected = float(CallbackId * CallbackId),
        ?assert(abs(Result - Expected) < 0.001)
    end, Results).
