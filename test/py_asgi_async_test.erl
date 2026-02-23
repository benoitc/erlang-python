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

-module(py_asgi_async_test).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Test fixtures
%% ============================================================================

setup() ->
    {ok, _} = application:ensure_all_started(erlang_python),
    timer:sleep(100),
    %% Ensure priv dir is in Python path (for test_asgi_apps module)
    PrivDir = code:priv_dir(erlang_python),
    PathCode = iolist_to_binary([
        "import sys\n",
        "priv_dir = '", PrivDir, "'\n",
        "if priv_dir not in sys.path:\n",
        "    sys.path.insert(0, priv_dir)\n"
    ]),
    ok = py:exec(PathCode),
    ok.

cleanup(_) ->
    ok.

%% ============================================================================
%% Tests
%% ============================================================================

asgi_async_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"run_async returns ref", fun test_run_async_returns_ref/0},
      {"await_response returns result", fun test_await_response_returns_result/0},
      {"echo body app", fun test_echo_body_app/0},
      {"concurrent async requests", fun test_concurrent_requests/0}
     ]}.

test_run_async_returns_ref() ->
    Scope = #{
        type => <<"http">>,
        method => <<"GET">>,
        path => <<"/">>
    },
    Result = py_asgi:run_async(<<"test_asgi_apps">>, <<"test_asgi_app">>, Scope, <<>>),
    ?assertMatch({ok, Ref} when is_reference(Ref), Result).

test_await_response_returns_result() ->
    Scope = #{
        type => <<"http">>,
        method => <<"GET">>,
        path => <<"/">>
    },
    {ok, Ref} = py_asgi:run_async(<<"test_asgi_apps">>, <<"test_asgi_app">>, Scope, <<>>),
    Result = py_asgi:await_response(Ref, 5000),
    ?assertMatch({ok, {200, _, _}}, Result),
    {ok, {200, _Headers, Body}} = Result,
    ?assertEqual(<<"Hello, World!">>, Body).

test_echo_body_app() ->
    Scope = #{
        type => <<"http">>,
        method => <<"POST">>,
        path => <<"/echo">>
    },
    RequestBody = <<"Test request body">>,
    {ok, Ref} = py_asgi:run_async(<<"test_asgi_apps">>, <<"echo_body_app">>, Scope, RequestBody),
    Result = py_asgi:await_response(Ref, 5000),
    ?assertMatch({ok, {200, _, _}}, Result),
    {ok, {200, _Headers, Body}} = Result,
    ?assertEqual(RequestBody, Body).

test_concurrent_requests() ->
    Scope = #{
        type => <<"http">>,
        method => <<"GET">>,
        path => <<"/">>
    },
    NumRequests = 5,

    %% Submit all requests
    Refs = [begin
        {ok, Ref} = py_asgi:run_async(<<"test_asgi_apps">>, <<"test_asgi_app">>, Scope, <<>>),
        Ref
    end || _ <- lists:seq(1, NumRequests)],

    %% Await all responses
    Results = [py_asgi:await_response(Ref, 10000) || Ref <- Refs],

    %% Verify all succeeded
    ?assertEqual(NumRequests, length(Results)),
    lists:foreach(fun(R) ->
        ?assertMatch({ok, {200, _, <<"Hello, World!">>}}, R)
    end, Results).
