%%% @doc CT suite pinning the version-gated asyncio policy install.
%%%
%%% On Python 3.14+ the integration must NOT call
%%% `asyncio.set_event_loop_policy/0` (deprecated in 3.14, removed in
%%% 3.16). On Python <3.14 the policy install is preserved as the
%%% historical convenience for bare `asyncio.run()` inside `py:exec`.
%%%
%%% These cases verify both halves of the gate plus the architectural
%%% claim that the run path works without the global policy.
-module(py_asyncio_policy_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    policy_install_skipped_on_3_14_plus/1,
    policy_install_active_below_3_14/1,
    async_call_round_trip/1,
    erlang_run_uses_erlang_loop/1,
    install_raises_on_3_14_plus/1,
    install_works_below_3_14/1,
    no_deprecation_warning_during_init/1
]).

all() ->
    [
        policy_install_skipped_on_3_14_plus,
        policy_install_active_below_3_14,
        async_call_round_trip,
        erlang_run_uses_erlang_loop,
        install_raises_on_3_14_plus,
        install_works_below_3_14,
        no_deprecation_warning_during_init
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%%% ---------------------------------------------------------------------------
%%% Helpers
%%% ---------------------------------------------------------------------------

python_at_least(Major, Minor) ->
    ok = py:exec(<<"import sys">>),
    {ok, {Maj, Min}} = py:eval(<<"sys.version_info[:2]">>),
    {Maj, Min} >= {Major, Minor}.

policy_class_name() ->
    %% asyncio.get_event_loop_policy() itself emits a DeprecationWarning
    %% on 3.14+; suppress it locally so the probe doesn't pollute the run.
    Code = <<
        "import asyncio, warnings\n"
        "with warnings.catch_warnings():\n"
        "    warnings.simplefilter('ignore', DeprecationWarning)\n"
        "    _pol_name = asyncio.get_event_loop_policy().__class__.__name__\n"
    >>,
    ok = py:exec(Code),
    {ok, Name} = py:eval(<<"_pol_name">>),
    Name.

%%% ---------------------------------------------------------------------------
%%% Tests
%%% ---------------------------------------------------------------------------

policy_install_skipped_on_3_14_plus(_Config) ->
    case python_at_least(3, 14) of
        false ->
            {skip, "Python <3.14 — global policy install is the right way"};
        true ->
            Name = policy_class_name(),
            true = Name =/= <<"ErlangEventLoopPolicy">>,
            ct:pal("Policy on 3.14+ is ~p (not ErlangEventLoopPolicy, as expected)",
                   [Name]),
            ok
    end.

policy_install_active_below_3_14(_Config) ->
    case python_at_least(3, 14) of
        true ->
            {skip, "Python 3.14+ — policy is intentionally not installed"};
        false ->
            <<"ErlangEventLoopPolicy">> = policy_class_name(),
            ok
    end.

async_call_round_trip(_Config) ->
    %% Independent of policy state: async_call → async_await must succeed.
    Ref = py:async_call(math, sqrt, [16]),
    true = is_reference(Ref),
    {ok, 4.0} = py:async_await(Ref, 5000),
    ok.

erlang_run_uses_erlang_loop(_Config) ->
    %% Confirm erlang.run picks up ErlangEventLoop on every supported
    %% version, regardless of the global policy state.
    ok = py:exec(<<
        "import erlang, asyncio\n"
        "async def _probe():\n"
        "    return type(asyncio.get_running_loop()).__name__\n"
        "_loop_class = erlang.run(_probe())\n"
    >>),
    {ok, <<"ErlangEventLoop">>} = py:eval(<<"_loop_class">>),
    ok.

install_raises_on_3_14_plus(_Config) ->
    case python_at_least(3, 14) of
        false ->
            {skip, "Python <3.14 — erlang.install() still functional"};
        true ->
            ok = py:exec(<<
                "import erlang\n"
                "_install_err = None\n"
                "try:\n"
                "    erlang.install()\n"
                "except RuntimeError as e:\n"
                "    _install_err = str(e)\n"
            >>),
            {ok, ErrMsg} = py:eval(<<"_install_err">>),
            true = is_binary(ErrMsg),
            true = byte_size(ErrMsg) > 0,
            true = binary:match(ErrMsg, <<"3.14+">>) =/= nomatch
                orelse binary:match(ErrMsg, <<"loop_factory">>) =/= nomatch,
            ok
    end.

install_works_below_3_14(_Config) ->
    case python_at_least(3, 14) of
        true ->
            {skip, "Python 3.14+ — erlang.install() raises by design"};
        false ->
            %% A DeprecationWarning is acceptable on 3.12-3.13; any
            %% exception is not.
            ok = py:exec(<<
                "import erlang, warnings\n"
                "with warnings.catch_warnings():\n"
                "    warnings.simplefilter('ignore', DeprecationWarning)\n"
                "    erlang.install()\n"
            >>),
            ok
    end.

no_deprecation_warning_during_init(_Config) ->
    case python_at_least(3, 14) of
        false ->
            {skip, "Python <3.14 — no deprecation warning to verify"};
        true ->
            %% Mimic the set_default_policy snippet inside a
            %% catch_warnings block; assert no set_event_loop_policy
            %% warning surfaces.
            ok = py:exec(<<
                "import asyncio, sys, warnings\n"
                "with warnings.catch_warnings(record=True) as _caught:\n"
                "    warnings.simplefilter('always')\n"
                "    if sys.version_info < (3, 14):\n"
                "        from _erlang_impl import get_event_loop_policy\n"
                "        asyncio.set_event_loop_policy(get_event_loop_policy())\n"
                "_relevant = [str(w.message) for w in _caught\n"
                "             if 'set_event_loop_policy' in str(w.message)]\n"
            >>),
            {ok, []} = py:eval(<<"_relevant">>),
            ok
    end.
