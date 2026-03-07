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

-module(py_venv_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").

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
    test_ensure_venv_creates_venv/1,
    test_ensure_venv_activates_existing/1,
    test_ensure_venv_with_requirements/1,
    test_ensure_venv_force_recreate/1,
    test_activate_venv/1,
    test_deactivate_venv/1,
    test_venv_info/1
]).

all() ->
    [{group, venv_tests}].

groups() ->
    [{venv_tests, [sequence], [
        test_ensure_venv_creates_venv,
        test_ensure_venv_activates_existing,
        test_ensure_venv_with_requirements,
        test_ensure_venv_force_recreate,
        test_activate_venv,
        test_deactivate_venv,
        test_venv_info
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(erlang_python),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Get Python executable path from the running interpreter
    %% Note: sys.executable returns beam.smp when embedded, so we find the actual Python
    %% Use a single expression to avoid any exec issues
    Expr = <<"(lambda: next((p for p in [__import__('os').path.join(__import__('sys').prefix, 'bin', f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python3'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python')] if __import__('os').path.isfile(p)), 'python3'))()">>,
    {ok, PythonPath} = py:eval(Expr),
    [{python_path, binary_to_list(PythonPath)} | Config].

end_per_group(_Group, _Config) ->
    ok.

%% @private Create venv using the Python from config
create_test_venv(VenvPath, Config) ->
    PythonPath = ?config(python_path, Config),
    Cmd = PythonPath ++ " -m venv " ++ VenvPath,
    _ = os:cmd(Cmd),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Create unique temp directory for each test
    TempDir = filename:join(["/tmp", "py_venv_test_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    filelib:ensure_dir(filename:join(TempDir, "dummy")),
    [{temp_dir, TempDir} | Config].

end_per_testcase(_TestCase, Config) ->
    %% Clean up temp directory
    TempDir = ?config(temp_dir, Config),
    os:cmd("rm -rf " ++ TempDir),
    %% Deactivate any active venv
    py:deactivate_venv(),
    ok.

%%% ============================================================================
%%% Test Cases
%%% ============================================================================

test_ensure_venv_creates_venv(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),
    ReqFile = filename:join(TempDir, "requirements.txt"),

    %% Create empty requirements file
    ok = file:write_file(ReqFile, <<"# empty\n">>),

    %% ensure_venv should create the venv and activate it
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}]),

    %% Verify venv was created
    true = filelib:is_file(filename:join(VenvPath, "pyvenv.cfg")),

    %% Verify venv is active
    {ok, Info} = py:venv_info(),
    true = maps:get(<<"active">>, Info),
    ok.

test_ensure_venv_activates_existing(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),
    ReqFile = filename:join(TempDir, "requirements.txt"),

    %% Create empty requirements file
    ok = file:write_file(ReqFile, <<"# empty\n">>),

    %% Create venv first time
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}]),

    %% Deactivate
    ok = py:deactivate_venv(),
    {ok, Info1} = py:venv_info(),
    false = maps:get(<<"active">>, Info1),

    %% ensure_venv again should just activate existing venv (not recreate)
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}]),

    %% Verify venv is active again
    {ok, Info2} = py:venv_info(),
    true = maps:get(<<"active">>, Info2),
    ok.

test_ensure_venv_with_requirements(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),
    ReqFile = filename:join(TempDir, "requirements.txt"),

    %% Create requirements file with a simple package
    ok = file:write_file(ReqFile, <<"six\n">>),

    %% ensure_venv should create venv and install six
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}]),

    %% Verify six is importable
    {ok, Version} = py:eval(<<"__import__('six').__version__">>),
    true = is_binary(Version),
    ok.

test_ensure_venv_force_recreate(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),
    ReqFile = filename:join(TempDir, "requirements.txt"),

    %% Create empty requirements
    ok = file:write_file(ReqFile, <<"# empty\n">>),

    %% Create venv first time
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}]),

    %% Get the pyvenv.cfg mtime
    {ok, Info1} = file:read_file_info(filename:join(VenvPath, "pyvenv.cfg")),
    Mtime1 = Info1#file_info.mtime,

    %% Wait a bit
    timer:sleep(1100),

    %% Force recreate
    ok = py:deactivate_venv(),
    ok = py:ensure_venv(VenvPath, ReqFile, [{installer, pip}, force]),

    %% Verify mtime changed (venv was recreated)
    {ok, Info2} = file:read_file_info(filename:join(VenvPath, "pyvenv.cfg")),
    Mtime2 = Info2#file_info.mtime,
    true = Mtime2 > Mtime1,
    ok.

test_activate_venv(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),

    %% Create venv manually using the same Python we're linked against
    ok = create_test_venv(VenvPath, Config),

    %% Activate it
    ok = py:activate_venv(VenvPath),

    %% Verify active
    {ok, Info} = py:venv_info(),
    true = maps:get(<<"active">>, Info),
    VenvBin = list_to_binary(VenvPath),
    VenvBin = maps:get(<<"venv_path">>, Info),
    ok.

test_deactivate_venv(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),

    %% Create and activate venv using the same Python we're linked against
    ok = create_test_venv(VenvPath, Config),
    ok = py:activate_venv(VenvPath),

    %% Verify active
    {ok, Info1} = py:venv_info(),
    true = maps:get(<<"active">>, Info1),

    %% Deactivate
    ok = py:deactivate_venv(),

    %% Verify not active
    {ok, Info2} = py:venv_info(),
    false = maps:get(<<"active">>, Info2),
    ok.

test_venv_info(Config) ->
    TempDir = ?config(temp_dir, Config),
    VenvPath = filename:join(TempDir, "venv"),

    %% Before activation, should be inactive
    {ok, Info1} = py:venv_info(),
    false = maps:get(<<"active">>, Info1),

    %% Create and activate using the same Python we're linked against
    ok = create_test_venv(VenvPath, Config),
    ok = py:activate_venv(VenvPath),

    %% After activation, should have all info
    {ok, Info2} = py:venv_info(),
    true = maps:get(<<"active">>, Info2),
    true = is_binary(maps:get(<<"venv_path">>, Info2)),
    true = is_binary(maps:get(<<"site_packages">>, Info2)),
    true = is_list(maps:get(<<"sys_path">>, Info2)),
    ok.
