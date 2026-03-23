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

%%% @doc Tests for py_preload module.
-module(py_preload_SUITE).

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
    set_get_clear_test/1,
    has_preload_test/1,
    preload_variable_inherited_test/1,
    preload_function_inherited_test/1,
    preload_import_inherited_test/1,
    preload_isolation_test/1,
    preload_new_context_inherits_test/1,
    clear_preload_new_context_test/1
]).

all() ->
    [
        {group, api_tests},
        {group, inheritance_tests}
    ].

groups() ->
    [
        {api_tests, [sequence], [
            set_get_clear_test,
            has_preload_test
        ]},
        {inheritance_tests, [sequence], [
            preload_variable_inherited_test,
            preload_function_inherited_test,
            preload_import_inherited_test,
            preload_isolation_test,
            preload_new_context_inherits_test,
            clear_preload_new_context_test
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    %% Clear any preload code left over
    py_preload:clear_code(),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure clean state before each test
    py_preload:clear_code(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Clean up after each test
    py_preload:clear_code(),
    ok.

%% =============================================================================
%% API Tests
%% =============================================================================

set_get_clear_test(_Config) ->
    %% Initially no code is set
    undefined = py_preload:get_code(),

    %% Set code
    Code = <<"X = 42">>,
    ok = py_preload:set_code(Code),

    %% Get code
    Code = py_preload:get_code(),

    %% Set different code (overwrite)
    Code2 = <<"Y = 100">>,
    ok = py_preload:set_code(Code2),
    Code2 = py_preload:get_code(),

    %% Clear code
    ok = py_preload:clear_code(),
    undefined = py_preload:get_code(),

    %% Clear again should not error
    ok = py_preload:clear_code(),
    undefined = py_preload:get_code().

has_preload_test(_Config) ->
    %% Initially no preload
    false = py_preload:has_preload(),

    %% Set code
    ok = py_preload:set_code(<<"X = 1">>),
    true = py_preload:has_preload(),

    %% Clear code
    ok = py_preload:clear_code(),
    false = py_preload:has_preload().

%% =============================================================================
%% Inheritance Tests
%% =============================================================================

preload_variable_inherited_test(_Config) ->
    %% Set preload code with a variable
    ok = py_preload:set_code(<<"PRELOADED = 42">>),

    %% Create context and check variable is available
    {ok, Ctx} = py_context:new(#{mode => worker}),
    try
        {ok, 42} = py:eval(Ctx, <<"PRELOADED">>)
    after
        py_context:stop(Ctx)
    end.

preload_function_inherited_test(_Config) ->
    %% Set preload code with a function
    ok = py_preload:set_code(<<"
def double(x):
    return x * 2

def add(a, b):
    return a + b
">>),

    %% Create context and check functions work
    {ok, Ctx} = py_context:new(#{mode => worker}),
    try
        {ok, 10} = py:eval(Ctx, <<"double(5)">>),
        {ok, 7} = py:eval(Ctx, <<"add(3, 4)">>)
    after
        py_context:stop(Ctx)
    end.

preload_import_inherited_test(_Config) ->
    %% Set preload code with imports
    ok = py_preload:set_code(<<"
import json
import os
">>),

    %% Create context and check imports are available
    {ok, Ctx} = py_context:new(#{mode => worker}),
    try
        %% json should be available
        {ok, <<"{\"a\": 1}">>} = py:eval(Ctx, <<"json.dumps({'a': 1})">>),
        %% os should be available
        {ok, _} = py:eval(Ctx, <<"os.getcwd()">>)
    after
        py_context:stop(Ctx)
    end.

preload_isolation_test(_Config) ->
    %% Set preload code
    ok = py_preload:set_code(<<"PRELOADED = 42">>),

    %% Create first context and modify the variable
    {ok, Ctx1} = py_context:new(#{mode => worker}),
    try
        {ok, 42} = py:eval(Ctx1, <<"PRELOADED">>),
        ok = py:exec(Ctx1, <<"PRELOADED = 100">>),
        {ok, 100} = py:eval(Ctx1, <<"PRELOADED">>),

        %% Create second context - should have original value
        {ok, Ctx2} = py_context:new(#{mode => worker}),
        try
            {ok, 42} = py:eval(Ctx2, <<"PRELOADED">>)
        after
            py_context:stop(Ctx2)
        end
    after
        py_context:stop(Ctx1)
    end.

preload_new_context_inherits_test(_Config) ->
    %% Set preload code
    ok = py_preload:set_code(<<"
SHARED_CONFIG = {'debug': True, 'version': '1.0'}

def get_config_value(key):
    return SHARED_CONFIG.get(key)
">>),

    %% Create multiple contexts - all should have preload
    Ctxs = [begin
        {ok, C} = py_context:new(#{mode => worker}),
        C
    end || _ <- lists:seq(1, 3)],

    try
        lists:foreach(fun(Ctx) ->
            {ok, true} = py:eval(Ctx, <<"get_config_value('debug')">>),
            {ok, <<"1.0">>} = py:eval(Ctx, <<"get_config_value('version')">>)
        end, Ctxs)
    after
        lists:foreach(fun(Ctx) -> py_context:stop(Ctx) end, Ctxs)
    end.

clear_preload_new_context_test(_Config) ->
    %% Set preload code
    ok = py_preload:set_code(<<"PRELOADED = 42">>),

    %% Create context with preload
    {ok, Ctx1} = py_context:new(#{mode => worker}),
    try
        {ok, 42} = py:eval(Ctx1, <<"PRELOADED">>)
    after
        py_context:stop(Ctx1)
    end,

    %% Clear preload
    ok = py_preload:clear_code(),

    %% New context should NOT have preloaded variable
    {ok, Ctx2} = py_context:new(#{mode => worker}),
    try
        {error, _} = py:eval(Ctx2, <<"PRELOADED">>)
    after
        py_context:stop(Ctx2)
    end.
