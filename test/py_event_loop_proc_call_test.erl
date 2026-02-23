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

-module(py_event_loop_proc_call_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    LoopRef = make_ref(),
    {ok, Pid} = py_event_loop_proc:start_link(LoopRef),
    Pid.

cleanup(Pid) ->
    py_event_loop_proc:stop(Pid),
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

call_result_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(Pid) ->
         [
          {"register and receive result", fun() -> test_register_receive_result(Pid) end},
          {"register and receive error", fun() -> test_register_receive_error(Pid) end},
          {"unregister before result", fun() -> test_unregister_before_result(Pid) end},
          {"multiple concurrent callbacks", fun() -> test_concurrent_callbacks(Pid) end}
         ]
     end}.

test_register_receive_result(Pid) ->
    CallbackId = 1,
    Ref = make_ref(),

    %% Register call handler
    ok = py_event_loop_proc:register_call(Pid, CallbackId, Ref),

    %% Simulate result delivery (would come from NIF in real use)
    Pid ! {call_result, CallbackId, {ok, <<"hello">>}},

    %% Should receive result
    receive
        {py_result, Ref, Result} ->
            ?assertEqual({ok, <<"hello">>}, Result)
    after 1000 ->
        ?assert(false)
    end.

test_register_receive_error(Pid) ->
    CallbackId = 2,
    Ref = make_ref(),

    %% Register call handler
    ok = py_event_loop_proc:register_call(Pid, CallbackId, Ref),

    %% Simulate error delivery
    Pid ! {call_error, CallbackId, {python_error, "NameError", "name 'x' is not defined"}},

    %% Should receive error
    receive
        {py_error, Ref, Error} ->
            ?assertEqual({python_error, "NameError", "name 'x' is not defined"}, Error)
    after 1000 ->
        ?assert(false)
    end.

test_unregister_before_result(Pid) ->
    CallbackId = 3,
    Ref = make_ref(),

    %% Register call handler
    ok = py_event_loop_proc:register_call(Pid, CallbackId, Ref),

    %% Unregister before result arrives
    ok = py_event_loop_proc:unregister_call(Pid, CallbackId),

    %% Give time for unregister to be processed
    timer:sleep(10),

    %% Simulate result delivery (should be ignored)
    Pid ! {call_result, CallbackId, {ok, <<"ignored">>}},

    %% Should NOT receive result (since unregistered)
    receive
        {py_result, Ref, _} ->
            ?assert(false)
    after 100 ->
        ok
    end.

test_concurrent_callbacks(Pid) ->
    Self = self(),
    NumCallbacks = 50,

    %% Register multiple callbacks
    Refs = lists:map(fun(CallbackId) ->
        Ref = make_ref(),
        ok = py_event_loop_proc:register_call(Pid, CallbackId, Ref),
        {CallbackId, Ref}
    end, lists:seq(100, 100 + NumCallbacks - 1)),

    %% Spawn processes to send results concurrently
    lists:foreach(fun({CallbackId, _Ref}) ->
        spawn(fun() ->
            Pid ! {call_result, CallbackId, {ok, CallbackId * 2}},
            Self ! {sent, CallbackId}
        end)
    end, Refs),

    %% Wait for all sends
    lists:foreach(fun({CallbackId, _}) ->
        receive {sent, CallbackId} -> ok after 1000 -> error({timeout_send, CallbackId}) end
    end, Refs),

    %% Collect all results
    Results = lists:map(fun({CallbackId, Ref}) ->
        receive
            {py_result, Ref, {ok, Value}} ->
                ?assertEqual(CallbackId * 2, Value),
                {CallbackId, Value}
        after 1000 ->
            error({timeout, CallbackId})
        end
    end, Refs),

    %% Verify we got all results
    ?assertEqual(NumCallbacks, length(Results)).
