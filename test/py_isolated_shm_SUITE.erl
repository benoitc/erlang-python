%%% @doc Common Test suite for py_shm: shared memory regions between Erlang
%%% and Python contexts, in worker and isolated mode.
-module(py_isolated_shm_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    test_available/1,
    test_erlang_round_trip/1,
    test_bounds/1,
    test_close_idempotent_binary_survives/1,
    test_owner_death_closes/1,
    test_unknown_handle/1,
    test_pass_to_python/1,
    test_nested_in_structure/1,
    test_python_sees_later_write/1,
    test_python_writes_erlang_reads/1,
    test_returned_handle/1,
    test_mapped_once/1,
    test_numpy/1,
    test_two_contexts_share/1,
    test_mixed_pool_share/1,
    test_size_mismatch_refused/1,
    test_closed_wrapper_raises/1,
    test_read_only_handle/1,
    test_restart_remaps/1,
    test_churn_no_leak/1
]).

-define(MOD, py_test_isolated_shm).
-define(MB, (1024 * 1024)).

all() ->
    [{group, erlang}, {group, worker}, {group, isolated}, {group, isolated_only}].

groups() ->
    ErlangOnly = [
        test_available,
        test_erlang_round_trip,
        test_bounds,
        test_close_idempotent_binary_survives,
        test_owner_death_closes,
        test_unknown_handle
    ],
    Both = [
        test_pass_to_python,
        test_nested_in_structure,
        test_python_sees_later_write,
        test_python_writes_erlang_reads,
        test_returned_handle,
        test_mapped_once,
        test_numpy,
        test_two_contexts_share,
        test_size_mismatch_refused,
        test_closed_wrapper_raises,
        test_read_only_handle
    ],
    IsolatedOnly = [
        test_mixed_pool_share,
        test_restart_remaps,
        test_churn_no_leak
    ],
    [{erlang, [], ErlangOnly},
     {worker, [], Both},
     {isolated, [], Both},
     {isolated_only, [], IsolatedOnly}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    case py_shm:available() of
        true -> [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config];
        false -> {skip, "iommap not available"}
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(erlang, Config) -> Config;
init_per_group(isolated_only, Config) -> [{mode, isolated} | Config];
init_per_group(Mode, Config) -> [{mode, Mode} | Config].

end_per_group(_Group, _Config) ->
    ok.

%%% ============================================================================
%%% Erlang side only
%%% ============================================================================

test_available(_Config) ->
    true = py_shm:available(),
    ok.

test_erlang_round_trip(_Config) ->
    {ok, Shm} = py_shm:new(?MB),
    ?MB = py_shm:size(Shm),
    {'$py_shm', _, Path, ?MB} = Shm,
    true = filelib:is_file(Path),
    Data = crypto:strong_rand_bytes(4096),
    ok = py_shm:write(Shm, 100, Data),
    {ok, Data} = py_shm:read(Shm, 100, 4096),
    Data = py_shm:binary(Shm, 100, 4096),
    {ok, #{size := ?MB}} = py_shm:info(Shm),
    ok = py_shm:close(Shm),
    false = filelib:is_file(Path),
    ok.

test_bounds(_Config) ->
    {ok, Shm} = py_shm:new(4096),
    {error, out_of_bounds} = py_shm:write(Shm, 4000, <<0:(200 * 8)>>),
    {error, out_of_bounds} = py_shm:read(Shm, 4000, 200),
    ok = py_shm:write(Shm, 4000, <<0:(96 * 8)>>),
    ok = py_shm:close(Shm),
    ok.

test_close_idempotent_binary_survives(_Config) ->
    {ok, Shm} = py_shm:new(4096),
    ok = py_shm:write(Shm, 0, <<"still here">>),
    Bin = py_shm:binary(Shm, 0, 10),
    ok = py_shm:close(Shm),
    ok = py_shm:close(Shm),
    <<"still here">> = Bin,
    {error, closed} = py_shm:write(Shm, 0, <<"x">>),
    {error, closed} = py_shm:info(Shm),
    ok.

test_owner_death_closes(_Config) ->
    Self = self(),
    Owner = spawn(fun() ->
        {ok, Shm} = py_shm:new(4096),
        Self ! {shm, Shm},
        receive die -> ok end
    end),
    Shm = receive {shm, S} -> S after 5000 -> ct:fail(no_shm) end,
    {'$py_shm', _, Path, _} = Shm,
    true = filelib:is_file(Path),
    Owner ! die,
    wait_until(fun() -> not filelib:is_file(Path) end, 5000),
    {error, closed} = py_shm:info(Shm),
    ok.

test_unknown_handle(_Config) ->
    Fake = {'$py_shm', 999999999, <<"/nonexistent">>, 10},
    {error, closed} = py_shm:read(Fake, 0, 1),
    ok = py_shm:close(Fake),
    ok.

%%% ============================================================================
%%% Erlang <-> Python (worker and isolated)
%%% ============================================================================

test_pass_to_python(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(?MB),
    Data = binary:copy(<<7>>, 1000),
    ok = py_shm:write(Shm, 0, Data),
    {ok, <<"SharedMemory">>} = py_context:call(C, ?MOD, kind, [Shm]),
    {ok, ?MB} = py_context:call(C, ?MOD, shm_len, [Shm]),
    {ok, 7000} = py_context:call(C, ?MOD, shm_sum, [Shm, 1000]),
    {ok, Data} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 1000]),
    ok = py_shm:close(Shm),
    stop(C).

test_nested_in_structure(Config) ->
    C = new_ctx(Config),
    {ok, A} = py_shm:new(4096),
    {ok, B} = py_shm:new(8192),
    {ok, [4096, 8192]} = py_context:call(C, ?MOD, shm_in_structure,
                                         [#{regions => [A, B], label => x}]),
    py_shm:close(A), py_shm:close(B),
    stop(C).

test_python_sees_later_write(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    ok = py_shm:write(Shm, 0, <<"first">>),
    {ok, <<"first">>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 5]),
    ok = py_shm:write(Shm, 0, <<"later">>),
    {ok, <<"later">>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 5]),
    py_shm:close(Shm),
    stop(C).

test_python_writes_erlang_reads(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(?MB),
    {ok, 11} = py_context:call(C, ?MOD, shm_write, [Shm, 10, {bytes, <<"from python">>}]),
    <<"from python">> = py_shm:binary(Shm, 10, 11),
    {ok, ?MB} = py_context:call(C, ?MOD, shm_fill, [Shm, 42, ?MB]),
    Bin = py_shm:binary(Shm, 0, ?MB),
    Bin = binary:copy(<<42>>, ?MB),
    py_shm:close(Shm),
    stop(C).

test_returned_handle(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    {ok, Shm} = py_context:call(C, ?MOD, shm_identity, [Shm]),
    py_shm:close(Shm),
    stop(C).

test_mapped_once(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    {ok, N0} = py_context:call(C, ?MOD, map_count, []),
    {ok, _} = py_context:call(C, ?MOD, shm_len, [Shm]),
    {ok, N1} = py_context:call(C, ?MOD, map_count, []),
    {ok, _} = py_context:call(C, ?MOD, shm_len, [Shm]),
    {ok, N2} = py_context:call(C, ?MOD, map_count, []),
    N1 = N0 + 1,
    N1 = N2,
    py_shm:close(Shm),
    stop(C).

test_numpy(Config) ->
    C = new_ctx(Config),
    case py_context:eval(C, <<"__import__('importlib.util').util.find_spec('numpy') is not None">>) of
        {ok, true} ->
            {ok, Shm} = py_shm:new(?MB),
            ok = py_shm:write(Shm, 0, binary:copy(<<3>>, 1000)),
            {ok, 3000} = py_context:call(C, ?MOD, shm_numpy_sum, [Shm, 1000]),
            {ok, Expected} = py_context:call(C, ?MOD, shm_numpy_write, [Shm, 256]),
            Expected = lists:sum(lists:seq(0, 255)),
            Bin = py_shm:binary(Shm, 0, 256),
            Bin = list_to_binary(lists:seq(0, 255)),
            py_shm:close(Shm),
            stop(C);
        _ ->
            stop(C),
            {skip, "numpy not installed"}
    end.

test_two_contexts_share(Config) ->
    C1 = new_ctx(Config),
    C2 = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    {ok, 5} = py_context:call(C1, ?MOD, shm_write, [Shm, 0, {bytes, <<"hello">>}]),
    {ok, <<"hello">>} = py_context:call(C2, ?MOD, shm_read, [Shm, 0, 5]),
    py_shm:close(Shm),
    stop(C1), stop(C2).

test_mixed_pool_share(Config) ->
    Iso = new_ctx(Config),
    {ok, W} = py_context:new(#{mode => worker}),
    ok = py_context:exec(W, add_path(Config)),
    {ok, Shm} = py_shm:new(4096),
    {ok, 6} = py_context:call(W, ?MOD, shm_write, [Shm, 0, {bytes, <<"worker">>}]),
    {ok, <<"worker">>} = py_context:call(Iso, ?MOD, shm_read, [Shm, 0, 6]),
    {ok, 8} = py_context:call(Iso, ?MOD, shm_write, [Shm, 0, {bytes, <<"isolated">>}]),
    {ok, <<"isolated">>} = py_context:call(W, ?MOD, shm_read, [Shm, 0, 8]),
    py_shm:close(Shm),
    py_context:stop(W),
    stop(Iso).

%% @doc A handle whose file has a different size than it claims is refused
%% on map (no SIGBUS later).
test_size_mismatch_refused(Config) ->
    C = new_ctx(Config),
    {ok, {'$py_shm', Id, Path, _} = Shm} = py_shm:new(4096),
    Lie = {'$py_shm', Id, Path, 8192},
    case py_context:call(C, ?MOD, shm_len, [Lie]) of
        {error, {'RuntimeError', _}} -> ok;          %% isolated: raised in the child
        {error, arg_conversion_failed} -> ok         %% embedded: conversion refused
    end,
    py_shm:close(Shm),
    stop(C).

test_closed_wrapper_raises(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    {ok, <<"closed">>} = py_context:call(C, ?MOD, shm_closed_access, [Shm]),
    %% A fresh mapping is made on the next use
    {ok, 4096} = py_context:call(C, ?MOD, shm_len, [Shm]),
    py_shm:close(Shm),
    stop(C).

%% @doc A read-only handle: Python reads it, cannot write, Erlang still can.
test_read_only_handle(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096, #{writable => false}),
    {'$py_shm_ro', _, _, 4096} = Shm,
    ok = py_shm:write(Shm, 0, <<"erlang wrote">>),
    {ok, <<"erlang wrote">>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 12]),
    {ok, <<"read_only">>} = py_context:call(C, ?MOD, shm_write_readonly, [Shm]),
    <<"erlang wrote">> = py_shm:binary(Shm, 0, 12),
    %% A writable handle downgraded for one callee
    {ok, Rw} = py_shm:new(4096),
    Ro = py_shm:read_only(Rw),
    {ok, <<"read_only">>} = py_context:call(C, ?MOD, shm_write_readonly, [Ro]),
    {ok, 3} = py_context:call(C, ?MOD, shm_write, [Rw, 0, {bytes, <<"abc">>}]),
    py_shm:close(Shm), py_shm:close(Rw),
    stop(C).

test_restart_remaps(Config) ->
    C = new_ctx(Config),
    {ok, Shm} = py_shm:new(4096),
    ok = py_shm:write(Shm, 0, <<"persist">>),
    {ok, <<"persist">>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 7]),
    ok = py_context:kill(C),
    {ok, <<"persist">>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 7]),
    py_shm:close(Shm),
    stop(C).

test_churn_no_leak(Config) ->
    C = new_ctx(Config),
    Dir = py_shm:private_dir(),
    Files0 = length(filelib:wildcard(filename:join(Dir, "shm_*"))),
    Regions0 = ets:info(py_shm_regions, size),
    lists:foreach(fun(I) ->
        {ok, Shm} = py_shm:new(64 * 1024),
        ok = py_shm:write(Shm, 0, <<I:32>>),
        {ok, <<I:32>>} = py_context:call(C, ?MOD, shm_read, [Shm, 0, 4]),
        ok = py_shm:close(Shm)
    end, lists:seq(1, 200)),
    Files1 = length(filelib:wildcard(filename:join(Dir, "shm_*"))),
    Files0 = Files1,
    Regions0 = ets:info(py_shm_regions, size),
    stop(C).

%%% ============================================================================
%%% Helpers
%%% ============================================================================

new_ctx(Config) ->
    Mode = ?config(mode, Config),
    TestDir = ?config(test_dir, Config),
    {ok, C} = py_context:new(#{mode => Mode, paths => [TestDir]}),
    case Mode of
        worker -> ok = py_context:exec(C, add_path(Config));
        _ -> ok
    end,
    C.

add_path(Config) ->
    TestDir = ?config(test_dir, Config),
    iolist_to_binary(io_lib:format(
        "import sys\nif '~s' not in sys.path: sys.path.insert(0, '~s')", [TestDir, TestDir])).

stop(C) ->
    ok = py_context:stop(C),
    ok.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        false ->
            erlang:monotonic_time(millisecond) < Deadline orelse ct:fail(condition_not_met),
            timer:sleep(50),
            wait_loop(Fun, Deadline)
    end.
