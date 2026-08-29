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

%%% @doc Virtual environments: create, install dependencies, activate.
%%% Implementation of `py:ensure_venv/2,3', `py:activate_venv/1',
%%% `py:deactivate_venv/0', `py:venv_info/0' and `py:python_executable/0';
%%% use those. Owns the venv layout under the configured directory and the
%%% pip/uv invocation.
%%% @private
-module(py_venv).

-export([
    ensure_venv/2,
    ensure_venv/3,
    python_executable/0,
    activate_venv/1,
    deactivate_venv/0,
    venv_info/0
]).


%% @doc Ensure a virtual environment exists and activate it.
%%
%% Creates a venv at `Path' if it doesn't exist, installs dependencies from
%% `RequirementsFile', and activates the venv.
%%
%% RequirementsFile can be:
%% - `"requirements.txt"' - standard pip requirements file
%% - `"pyproject.toml"' - PEP 621 project file (installs with -e .)
%%
%% Example:
%% ```
%% ok = py:ensure_venv("priv/venv", "requirements.txt").
%% '''
-spec ensure_venv(string() | binary(), string() | binary()) -> ok | {error, term()}.
ensure_venv(Path, RequirementsFile) ->
    ensure_venv(Path, RequirementsFile, []).

%% @doc Ensure a virtual environment exists with options.
%%
%% Options:
%% - `{extras, [string()]}' - Install optional dependencies (pyproject.toml)
%% - `{installer, uv | pip}' - Package installer (default: auto-detect)
%% - `{python, string()}' - Python executable for venv creation
%% - `force' - Recreate venv even if it exists
%%
%% Example:
%% ```
%% %% With pyproject.toml and dev extras
%% ok = py:ensure_venv("priv/venv", "pyproject.toml", [
%%     {extras, ["dev", "test"]}
%% ]).
%%
%% %% Force uv installer
%% ok = py:ensure_venv("priv/venv", "requirements.txt", [
%%     {installer, uv}
%% ]).
%% '''
-spec ensure_venv(string() | binary(), string() | binary(), list()) -> ok | {error, term()}.
ensure_venv(Path, RequirementsFile, Opts) ->
    PathStr = to_string(Path),
    ReqFileStr = to_string(RequirementsFile),
    Force = proplists:get_bool(force, Opts),
    %% Create venv if needed
    VenvReady = case venv_exists(PathStr) of
        true when not Force ->
            ok;
        _ ->
            create_venv(PathStr, Opts)
    end,
    case VenvReady of
        ok ->
            %% Always install/update dependencies (pip/uv skip existing)
            case install_deps(PathStr, ReqFileStr, Opts) of
                ok ->
                    activate_venv(PathStr);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Check if venv exists by looking for pyvenv.cfg
-spec venv_exists(string()) -> boolean().
venv_exists(Path) ->
    filelib:is_file(filename:join(Path, "pyvenv.cfg")).

%% @private Create a new virtual environment
-spec create_venv(string(), list()) -> ok | {error, term()}.
create_venv(Path, Opts) ->
    Installer = detect_installer(Opts),
    Python = case proplists:get_value(python, Opts, undefined) of
        undefined -> get_python_executable();
        P -> P
    end,
    case Installer of
        uv ->
            %% uv venv is faster, use --python to match the running interpreter
            run_cmd(uv_exe(), ["venv", "--python", Python, Path], []);
        pip ->
            run_cmd(Python, ["-m", "venv", Path], [])
    end.

%% @private Get the Python executable path
%% When embedded, sys.executable returns the embedding app (beam.smp)
%% so we reconstruct the path from sys.prefix and version info
%% @doc Path of the Python interpreter matching the embedded runtime.
%%
%% Reconstructed from `sys.prefix' (when embedded, `sys.executable' is the
%% VM). Used as the default interpreter of isolated contexts and for venvs.
-spec python_executable() -> string().
python_executable() ->
    get_python_executable().


-spec get_python_executable() -> string().
get_python_executable() ->
    %% Use a single expression to find the Python executable
    %% Searches for pythonX.Y, python3, python in sys.prefix/bin (Unix)
    %% or python.exe in sys.prefix (Windows)
    Expr = <<"(lambda: (__import__('os').path.join(__import__('sys').prefix, 'python.exe') if __import__('sys').platform == 'win32' and __import__('os').path.isfile(__import__('os').path.join(__import__('sys').prefix, 'python.exe')) else next((p for p in [__import__('os').path.join(__import__('sys').prefix, 'bin', f'python{__import__(\"sys\").version_info.major}.{__import__(\"sys\").version_info.minor}'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python3'), __import__('os').path.join(__import__('sys').prefix, 'bin', 'python')] if __import__('os').path.isfile(p)), 'python3')))()">>,
    case py:eval(Expr) of
        {ok, Path} when is_binary(Path) -> binary_to_list(Path);
        _ -> "python3"
    end.

%% @private Install dependencies from requirements file
-spec install_deps(string(), string(), list()) -> ok | {error, term()}.
install_deps(Path, RequirementsFile, Opts) ->
    Installer = detect_installer(Opts),
    {Exe, BaseArgs, PortOpts} = pip_command(Path, Installer),
    Extras = proplists:get_value(extras, Opts, []),

    %% Determine file type and build the install argument list (no shell).
    Args = case filename:extension(RequirementsFile) of
        ".txt" ->
            BaseArgs ++ ["install", "-r", RequirementsFile];
        ".toml" ->
            %% pyproject.toml - install as editable.
            %% filename:dirname returns "." for files without directory component
            InstallPath = filename:dirname(RequirementsFile),
            case Extras of
                [] ->
                    BaseArgs ++ ["install", "-e", InstallPath];
                _ ->
                    ExtrasStr = string:join(Extras, ","),
                    BaseArgs ++ ["install", "-e", InstallPath ++ "[" ++ ExtrasStr ++ "]"]
            end;
        _ ->
            BaseArgs ++ ["install", "-r", RequirementsFile]
    end,
    run_cmd(Exe, Args, PortOpts).

%% @private Detect which installer to use (uv or pip)
-spec detect_installer(list()) -> uv | pip.
detect_installer(Opts) ->
    case proplists:get_value(installer, Opts, auto) of
        auto ->
            case os:find_executable("uv") of
                false -> pip;
                _ -> uv
            end;
        Installer ->
            Installer
    end.

%% @private Resolve the installer into {Executable, BaseArgs, PortOpts}.
%% For uv the venv is selected via the VIRTUAL_ENV port env option (not a shell
%% prefix); for pip we use the venv's own pip binary.
-spec pip_command(string(), uv | pip) -> {string(), [string()], list()}.
pip_command(VenvPath, uv) ->
    {uv_exe(), ["pip"], [{env, [{"VIRTUAL_ENV", VenvPath}]}]};
pip_command(VenvPath, pip) ->
    PipExe = case os:type() of
        {win32, _} ->
            filename:join([VenvPath, "Scripts", "pip"]);
        _ ->
            filename:join([VenvPath, "bin", "pip"])
    end,
    {PipExe, [], []}.

%% @private Full path to the uv executable (falls back to the bare name).
-spec uv_exe() -> string().
uv_exe() ->
    case os:find_executable("uv") of
        false -> "uv";
        P -> P
    end.

%% @private Run an executable with an argv list (no shell) and return ok or error.
-spec run_cmd(string(), [string()], list()) -> ok | {error, term()}.
run_cmd(Exe, Args, ExtraOpts) ->
    case resolve_exe(Exe) of
        {error, _} = Err ->
            Err;
        ExeFull ->
            try open_port({spawn_executable, ExeFull},
                          [exit_status, stderr_to_stdout, binary, {args, Args} | ExtraOpts]) of
                Port -> collect_port(Port, [])
            catch
                error:Reason -> {error, {spawn_failed, Exe, Reason}}
            end
    end.

%% @private Resolve an executable name/path to a full path (spawn_executable does
%% not search PATH).
-spec resolve_exe(string()) -> string() | {error, term()}.
resolve_exe(Exe) ->
    case filename:pathtype(Exe) of
        absolute ->
            case filelib:is_file(Exe) of
                true -> Exe;
                false -> {error, {executable_not_found, Exe}}
            end;
        _ ->
            case os:find_executable(Exe) of
                false -> {error, {executable_not_found, Exe}};
                Found -> Found
            end
    end.

%% @private Collect a spawned port's output and exit status.
-spec collect_port(port(), [binary()]) -> ok | {error, term()}.
collect_port(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            collect_port(Port, [Data | Acc]);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, Code}} ->
            {error, {exit_code, Code, iolist_to_binary(lists:reverse(Acc))}}
    after 300000 ->
        try port_close(Port) catch _:_ -> ok end,
        {error, timeout}
    end.

%% @private Convert to string
-spec to_string(string() | binary()) -> string().
to_string(B) when is_binary(B) -> binary_to_list(B);
to_string(S) when is_list(S) -> S.

%% @doc Activate a Python virtual environment.
%% This modifies sys.path to use packages from the specified venv.
%% The venv path should be the root directory (containing bin/lib folders).
%%
%% `.pth' files in the venv's site-packages directory are processed, so
%% editable installs created by uv, pip, or any PEP 517/660 compliant tool
%% work correctly.  New paths are inserted at the front of sys.path so that
%% venv packages take priority over system packages.
%%
%% Example:
%% ```
%% ok = py:activate_venv(<<"/path/to/myenv">>).
%% {ok, _} = py:call(sentence_transformers, 'SentenceTransformer', [<<"all-MiniLM-L6-v2">>]).
%% '''
-spec activate_venv(string() | binary()) -> ok | {error, term()}.
activate_venv(VenvPath) ->
    VenvBin = py_util:to_binary(VenvPath),
    %% Find site-packages directory dynamically (venv may use different Python version)
    %% Uses a single expression to avoid multiline code issues
    FindSitePackages = <<"(lambda vp: __import__('os').path.join(vp, 'Lib', 'site-packages') if __import__('os').path.exists(__import__('os').path.join(vp, 'Lib', 'site-packages')) else next((sp for name in (__import__('os').listdir(__import__('os').path.join(vp, 'lib')) if __import__('os').path.isdir(__import__('os').path.join(vp, 'lib')) else []) if name.startswith('python') for sp in [__import__('os').path.join(vp, 'lib', name, 'site-packages')] if __import__('os').path.isdir(sp)), None))(_venv_path)">>,
    case py:eval(FindSitePackages, #{<<"_venv_path">> => VenvBin}) of
        {ok, SitePackages} when SitePackages =/= none, SitePackages =/= null ->
            activate_venv_with_site_packages(VenvBin, SitePackages);
        {ok, _} ->
            {error, {invalid_venv, no_site_packages_found}};
        Error ->
            Error
    end.

%% @private Activate venv with known site-packages path
activate_venv_with_site_packages(VenvBin, SitePackages) ->
    %% Verify site-packages exists
    case py:eval(<<"__import__('os').path.isdir(sp)">>, #{sp => SitePackages}) of
        {ok, true} ->
            %% Save original path if not already saved
            {ok, _} = py:eval(<<"setattr(__import__('sys'), '_original_path', __import__('sys').path.copy()) if not hasattr(__import__('sys'), '_original_path') else None">>),
            %% Set venv info
            {ok, _} = py:eval(<<"setattr(__import__('sys'), '_active_venv', vp)">>, #{vp => VenvBin}),
            {ok, _} = py:eval(<<"setattr(__import__('sys'), '_venv_site_packages', sp)">>, #{sp => SitePackages}),
            %% Add site-packages and process .pth files (editable installs)
            %% Note: We embed the site-packages path directly since exec doesn't support
            %% variables and sys attributes may not persist across calls in subinterpreters
            SitePackagesStr = binary_to_list(SitePackages),
            ExecCode = iolist_to_binary([
                <<"import site as _site, sys as _sys\n">>,
                <<"_sp = '">>, escape_python_string(SitePackagesStr), <<"'\n">>,
                <<"_b = frozenset(_sys.path)\n">>,
                <<"_site.addsitedir(_sp)\n">>,
                <<"_sys.path[:] = [p for p in _sys.path if p not in _b] + [p for p in _sys.path if p in _b]\n">>,
                <<"del _site, _sys, _b, _sp\n">>
            ]),
            ok = py:exec(ExecCode),
            ok;
        {ok, false} ->
            {error, {invalid_venv, SitePackages}};
        Error ->
            Error
    end.

%% @private Escape a string for embedding in Python code
escape_python_string(Str) ->
    lists:flatmap(fun($') -> "\\'";
                     ($\\) -> "\\\\";
                     (C) -> [C]
                  end, Str).




%% @doc Deactivate the current virtual environment.
%% Restores sys.path to its original state.
-spec deactivate_venv() -> ok | {error, term()}.
deactivate_venv() ->
    case py:eval(<<"hasattr(__import__('sys'), '_original_path')">>) of
        {ok, true} ->
            ok = py:exec(<<"import sys as _sys\n"
                         "_sys.path[:] = _sys._original_path\n"
                         "del _sys\n">>),
            {ok, _} = py:eval(<<"delattr(__import__('sys'), '_original_path')">>),
            {ok, _} = py:eval(<<"delattr(__import__('sys'), '_active_venv') if hasattr(__import__('sys'), '_active_venv') else None">>),
            {ok, _} = py:eval(<<"delattr(__import__('sys'), '_venv_site_packages') if hasattr(__import__('sys'), '_venv_site_packages') else None">>),
            ok;
        {ok, false} ->
            ok;
        Error ->
            Error
    end.

%% @doc Get information about the currently active virtual environment.
%% Returns a map with venv_path and site_packages, or none if no venv is active.
-spec venv_info() -> {ok, map() | none} | {error, term()}.
venv_info() ->
    %% Check both attributes exist to handle partial activation/deactivation state
    Code = <<"({'active': True, 'venv_path': __import__('sys')._active_venv, 'site_packages': __import__('sys')._venv_site_packages, 'sys_path': __import__('sys').path} if (hasattr(__import__('sys'), '_active_venv') and hasattr(__import__('sys'), '_venv_site_packages')) else {'active': False})">>,
    py:eval(Code).


