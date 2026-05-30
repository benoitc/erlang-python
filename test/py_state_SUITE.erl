%%% @doc Common Test suite for py_state shared-state store, focused on the
%%% optional entry cap and its accounting (memory-exhaustion resistance).
-module(py_state_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    cap_disabled_test/1,
    cap_enforced_test/1,
    cap_accounting_test/1,
    sentinel_protected_test/1
]).

-define(SIZE_KEY, '$py_state_size$').

all() ->
    [cap_disabled_test, cap_enforced_test, cap_accounting_test, sentinel_protected_test].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    application:stop(erlang_python),
    ok.

init_per_testcase(_TC, Config) ->
    application:unset_env(erlang_python, max_state_entries),
    py_state:clear(),
    Config.

end_per_testcase(_TC, _Config) ->
    application:unset_env(erlang_python, max_state_entries),
    py_state:clear(),
    ok.

%% @doc Default (infinity) preserves the previous unbounded behavior.
cap_disabled_test(_Config) ->
    [ok = py_state:store(N, N) || N <- lists:seq(1, 1000)],
    {ok, 500} = py_state:fetch(500),
    ok.

%% @doc A finite cap rejects new keys beyond the limit; overwrites don't consume
%% capacity.
cap_enforced_test(_Config) ->
    application:set_env(erlang_python, max_state_entries, 5),
    [ok = py_state:store({k, N}, N) || N <- lists:seq(1, 5)],
    {error, full} = py_state:store({k, 6}, 6),
    %% Overwriting an existing key does NOT consume capacity.
    ok = py_state:store({k, 3}, 30),
    {ok, 30} = py_state:fetch({k, 3}),
    {error, full} = py_state:store({k, 7}, 7),
    ok.

%% @doc Removing frees capacity; removing a missing key must not drift the count
%% (which would let the cap be bypassed or never trip).
cap_accounting_test(_Config) ->
    application:set_env(erlang_python, max_state_entries, 3),
    ok = py_state:store(a, 1),
    ok = py_state:store(b, 2),
    ok = py_state:store(c, 3),
    {error, full} = py_state:store(d, 4),
    %% Remove a missing key repeatedly: no phantom capacity freed.
    [py_state:remove(missing) || _ <- lists:seq(1, 10)],
    {error, full} = py_state:store(d, 4),
    %% Remove a real key: frees exactly one slot.
    ok = py_state:remove(a),
    ok = py_state:store(d, 4),
    {error, full} = py_state:store(e, 5),
    ok.

%% @doc The internal size sentinel can't be written or counter-corrupted by a
%% caller, and is hidden from keys/0 and fetch/1.
sentinel_protected_test(_Config) ->
    application:set_env(erlang_python, max_state_entries, 2),
    {error, reserved_key} = py_state:store(?SIZE_KEY, 9999),
    {'EXIT', _} = (catch py_state:incr(?SIZE_KEY, 100)),
    ok = py_state:store(x, 1),
    ok = py_state:store(y, 2),
    {error, full} = py_state:store(z, 3),
    {error, not_found} = py_state:fetch(?SIZE_KEY),
    false = lists:member(?SIZE_KEY, py_state:keys()),
    ok.
