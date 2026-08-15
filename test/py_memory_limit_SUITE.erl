%%% @doc Common Test suite for per-context memory caps.
%%%
%%% Caps are accounted from obmalloc arena traffic and enforced by raising
%%% MemoryError in the offending context. They require owngil mode and the
%%% runtime started with `enable_memory_limits' (set node-wide in
%%% test/test.config, since the allocator is hooked before Python starts).
-module(py_memory_limit_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    test_cap_raises_memory_error/1,
    test_context_survives_cap/1,
    test_cap_rearms_after_release/1,
    test_usage_is_reported/1,
    test_no_cap_is_unlimited/1,
    test_worker_mode_rejected/1,
    test_invalid_limit_rejected/1
]).

%% Allocates well past any cap used here: ~56 bytes per empty list.
-define(GREEDY, <<"_hog = [[] for _ in range(3000000)]">>).

-define(CAP, (64 * 1024 * 1024)).

all() -> [
    test_cap_raises_memory_error,
    test_context_survives_cap,
    test_cap_rearms_after_release,
    test_usage_is_reported,
    test_no_cap_is_unlimited,
    test_worker_mode_rejected,
    test_invalid_limit_rejected
].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    case py_nif:owngil_supported() of
        false ->
            {skip, "Memory limits require OWN_GIL (Python 3.14+)"};
        true ->
            %% The allocator is hooked before Python starts, so if another
            %% suite initialised the runtime without the flag we cannot turn
            %% it on here.
            case probe_enabled() of
                true -> Config;
                false -> {skip, "runtime started without enable_memory_limits"}
            end
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

%% @doc Allocating past the cap raises MemoryError in the Python code.
test_cap_raises_memory_error(_Config) ->
    {ok, Ctx} = py_context:new(#{mode => owngil, memory_limit => ?CAP}),
    {error, {'MemoryError', _}} = py_context:exec(Ctx, ?GREEDY),
    py_context:stop(Ctx),
    ok.

%% @doc The context is still usable after its cap has been hit.
test_context_survives_cap(_Config) ->
    {ok, Ctx} = py_context:new(#{mode => owngil, memory_limit => ?CAP}),
    {error, {'MemoryError', _}} = py_context:exec(Ctx, ?GREEDY),
    {ok, 4} = py_context:eval(Ctx, <<"2+2">>, #{}, 10000),
    py_context:stop(Ctx),
    ok.

%% @doc Once the memory is released the cap enforces again, rather than
%% latching after the first breach.
test_cap_rearms_after_release(_Config) ->
    {ok, Ctx} = py_context:new(#{mode => owngil, memory_limit => ?CAP}),
    {error, {'MemoryError', _}} = py_context:exec(Ctx, ?GREEDY),
    %% Whatever survived the unwinding goes away here
    _ = py_context:exec(Ctx, <<"_hog = None">>),
    {ok, _} = py_context:eval(Ctx, <<"__import__('gc').collect()">>, #{}, 30000),
    {error, {'MemoryError', _}} = py_context:exec(Ctx, ?GREEDY),
    py_context:stop(Ctx),
    ok.

%% @doc Usage is reported and grows with live objects.
test_usage_is_reported(_Config) ->
    {ok, Ctx} = py_context:new(#{mode => owngil}),
    Ref = py_context:get_nif_ref(Ctx),
    {ok, Before, 0} = py_nif:context_memory_usage(Ref),
    ok = py_context:exec(Ctx, <<"_keep = [[] for _ in range(500000)]">>),
    {ok, After, 0} = py_nif:context_memory_usage(Ref),
    true = After > Before,
    py_context:stop(Ctx),
    ok.

%% @doc Without a cap the same allocation succeeds.
test_no_cap_is_unlimited(_Config) ->
    {ok, Ctx} = py_context:new(#{mode => owngil}),
    ok = py_context:exec(Ctx, ?GREEDY),
    {ok, 3000000} = py_context:eval(Ctx, <<"len(_hog)">>, #{}, 10000),
    py_context:stop(Ctx),
    ok.

%% @doc Worker-mode contexts share the main interpreter, so a per-context cap
%% is refused rather than silently applied process-wide.
test_worker_mode_rejected(_Config) ->
    {error, memory_limit_requires_owngil} =
        py_context:new(#{mode => worker, memory_limit => ?CAP}),
    ok.

test_invalid_limit_rejected(_Config) ->
    {error, {invalid_memory_limit, -1}} =
        py_context:new(#{mode => owngil, memory_limit => -1}),
    {error, {invalid_memory_limit, <<"big">>}} =
        py_context:new(#{mode => owngil, memory_limit => <<"big">>}),
    ok.

%%% ============================================================================
%%% Helpers
%%% ============================================================================

%% Setting a zero cap is a no-op when limits are on and fails when they are off.
probe_enabled() ->
    case py_context:new(#{mode => owngil}) of
        {ok, Ctx} ->
            Ref = py_context:get_nif_ref(Ctx),
            Result = py_nif:context_set_memory_limit(Ref, 0),
            py_context:stop(Ctx),
            Result =:= ok;
        _ ->
            false
    end.
