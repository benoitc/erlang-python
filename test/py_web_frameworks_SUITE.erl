%%% @doc Common Test suite for deprecated ASGI/WSGI modules.
%%%
%%% Tests that py_asgi and py_wsgi modules are marked as deprecated.
-module(py_web_frameworks_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_asgi_deprecated/1,
    test_wsgi_deprecated/1
]).

%% ============================================================================
%% Common Test callbacks
%% ============================================================================

all() ->
    [
        test_asgi_deprecated,
        test_wsgi_deprecated
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
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
