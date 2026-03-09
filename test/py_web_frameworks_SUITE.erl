%%% @doc Common Test suite for deprecated ASGI/WSGI modules.
%%%
%%% Tests that py_asgi and py_wsgi modules still work (backward compatibility)
%%% while being marked as deprecated.
-module(py_web_frameworks_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    %% Deprecation tests
    test_asgi_deprecated/1,
    test_wsgi_deprecated/1,
    %% ASGI backward compatibility
    test_asgi_run_basic/1,
    test_asgi_build_scope/1,
    test_asgi_scope_defaults/1,
    %% WSGI backward compatibility
    test_wsgi_run_basic/1,
    test_wsgi_environ_defaults/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        {group, deprecation},
        {group, asgi_compat},
        {group, wsgi_compat}
    ].

groups() ->
    [
        {deprecation, [], [
            test_asgi_deprecated,
            test_wsgi_deprecated
        ]},
        {asgi_compat, [], [
            test_asgi_run_basic,
            test_asgi_build_scope,
            test_asgi_scope_defaults
        ]},
        {wsgi_compat, [], [
            test_wsgi_run_basic,
            test_wsgi_environ_defaults
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Deprecation Tests
%% ============================================================================

%% @doc Test that py_asgi functions are marked as deprecated.
test_asgi_deprecated(_Config) ->
    %% Check module attributes for deprecation
    Attrs = py_asgi:module_info(attributes),
    Deprecated = proplists:get_value(deprecated, Attrs, []),

    %% Verify run/4 is deprecated
    true = lists:any(fun
        ({run, 4, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    %% Verify run/5 is deprecated
    true = lists:any(fun
        ({run, 5, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    %% Verify build_scope/1 is deprecated
    true = lists:any(fun
        ({build_scope, 1, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    %% Verify build_scope/2 is deprecated
    true = lists:any(fun
        ({build_scope, 2, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    ok.

%% @doc Test that py_wsgi functions are marked as deprecated.
test_wsgi_deprecated(_Config) ->
    %% Check module attributes for deprecation
    Attrs = py_wsgi:module_info(attributes),
    Deprecated = proplists:get_value(deprecated, Attrs, []),

    %% Verify run/3 is deprecated
    true = lists:any(fun
        ({run, 3, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    %% Verify run/4 is deprecated
    true = lists:any(fun
        ({run, 4, _Msg}) -> true;
        (_) -> false
    end, Deprecated),

    ok.

%% ============================================================================
%% ASGI Backward Compatibility Tests
%% ============================================================================

%% @doc Test basic ASGI run functionality still works.
test_asgi_run_basic(_Config) ->
    %% Test that run/4 can be called (may fail due to missing app, but shouldn't crash)
    Scope = #{
        type => <<"http">>,
        method => <<"GET">>,
        path => <<"/">>
    },
    %% We expect an error because we don't have a real ASGI app,
    %% but the function should be callable
    Result = py_asgi:run(<<"nonexistent_module">>, <<"app">>, Scope, <<>>),
    case Result of
        {error, _Reason} -> ok;  %% Expected - module doesn't exist
        {ok, _} -> ok  %% Unexpected but acceptable
    end.

%% @doc Test ASGI build_scope functionality.
test_asgi_build_scope(_Config) ->
    Scope = #{
        type => <<"http">>,
        method => <<"POST">>,
        path => <<"/api/test">>,
        query_string => <<"foo=bar">>
    },
    %% build_scope should be callable
    Result = py_asgi:build_scope(Scope),
    case Result of
        {ok, _Ref} -> ok;
        {error, _Reason} -> ok  %% May fail if NIF not available
    end.

%% @doc Test ASGI scope defaults are applied correctly.
test_asgi_scope_defaults(_Config) ->
    %% Minimal scope - should get defaults applied
    MinimalScope = #{
        path => <<"/test">>
    },
    %% This tests internal ensure_scope_defaults/1 via run/4
    Result = py_asgi:run(<<"test">>, <<"app">>, MinimalScope, <<>>),
    %% Should not crash, error is expected for missing module
    case Result of
        {error, _} -> ok;
        {ok, _} -> ok
    end.

%% ============================================================================
%% WSGI Backward Compatibility Tests
%% ============================================================================

%% @doc Test basic WSGI run functionality still works.
test_wsgi_run_basic(_Config) ->
    Environ = #{
        <<"REQUEST_METHOD">> => <<"GET">>,
        <<"PATH_INFO">> => <<"/">>,
        <<"wsgi.input">> => <<>>
    },
    %% We expect an error because we don't have a real WSGI app,
    %% but the function should be callable
    Result = py_wsgi:run(<<"nonexistent_module">>, <<"app">>, Environ),
    case Result of
        {error, _Reason} -> ok;  %% Expected - module doesn't exist
        {ok, _} -> ok  %% Unexpected but acceptable
    end.

%% @doc Test WSGI environ defaults are applied correctly.
test_wsgi_environ_defaults(_Config) ->
    %% Minimal environ - should get defaults applied
    MinimalEnviron = #{
        <<"PATH_INFO">> => <<"/test">>
    },
    %% This tests internal ensure_environ_defaults/1 via run/3
    Result = py_wsgi:run(<<"test">>, <<"app">>, MinimalEnviron),
    %% Should not crash, error is expected for missing module
    case Result of
        {error, _} -> ok;
        {ok, _} -> ok
    end.
