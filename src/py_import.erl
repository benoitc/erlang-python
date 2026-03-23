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

%%% @doc Import and path registry for Python interpreters.
%%%
%%% This module manages the global import and path registries that are
%%% applied to all Python interpreters. Imports and paths are applied
%%% immediately to all running interpreters and stored for new interpreters.
%%%
%%% == Configuration ==
%%%
%%% Imports and paths can be configured in the application environment:
%%%
%%% ```
%%% {erlang_python, [
%%%     {imports, [{json, dumps}, {math, sqrt}, {os, getcwd}]},
%%%     {paths, ["/path/to/modules"]}
%%% ]}
%%% '''
%%%
%%% == Examples ==
%%%
%%% ```
%%% %% Register modules for import in all interpreters (immediate + future)
%%% ok = py_import:ensure_imported(json).
%%% ok = py_import:ensure_imported(math, sqrt).
%%%
%%% %% Add paths to sys.path in all interpreters (immediate + future)
%%% ok = py_import:add_path("/path/to/my/modules").
%%%
%%% %% Check registry contents
%%% Imports = py_import:all_imports().
%%% Paths = py_import:all_paths().
%%% '''
-module(py_import).

-export([
    init/0,
    %% Import registry
    ensure_imported/1,
    ensure_imported/2,
    is_imported/1,
    is_imported/2,
    all_imports/0,
    clear_imports/0,
    import_list/0,
    %% Path registry
    add_path/1,
    add_paths/1,
    all_paths/0,
    clear_paths/0,
    is_path_added/1
]).

-type py_module() :: atom() | binary() | string().
-type py_func() :: atom() | binary() | string().

-export_type([py_module/0, py_func/0]).

%% ETS table for global import registry
-define(IMPORT_REGISTRY, py_import_registry).

%% ETS table for global path registry (sys.path additions)
-define(PATH_REGISTRY, py_path_registry).

%%% ============================================================================
%%% Initialization
%%% ============================================================================

%% @doc Initialize the import and path registry ETS tables.
%%
%% This is called automatically during application startup.
%% Also loads imports and paths from application config.
%% Safe to call multiple times - does nothing if already initialized.
%%
%% @returns ok
-spec init() -> ok.
init() ->
    case ets:info(?IMPORT_REGISTRY) of
        undefined ->
            ets:new(?IMPORT_REGISTRY, [bag, public, named_table]),
            ok;
        _ ->
            ok
    end,
    case ets:info(?PATH_REGISTRY) of
        undefined ->
            ets:new(?PATH_REGISTRY, [ordered_set, public, named_table]),
            ok;
        _ ->
            ok
    end,
    load_config().

%%% ============================================================================
%%% Module Import Registry
%%% ============================================================================

%% @doc Register a module for import in all interpreters.
%%
%% Imports the module immediately in all running interpreters and adds it
%% to the registry for future interpreters.
%%
%% The `__main__' module is never cached.
%%
%% Example:
%% ```
%% ok = py_import:ensure_imported(json),
%% {ok, Result} = py:call(json, dumps, [Data]).
%% '''
%%
%% @param Module Python module name
%% @returns ok | {error, Reason}
-spec ensure_imported(py_module()) -> ok | {error, term()}.
ensure_imported(Module) ->
    ModuleBin = ensure_binary(Module),
    case ModuleBin of
        <<"__main__">> ->
            {error, main_not_cacheable};
        _ ->
            ets:insert(?IMPORT_REGISTRY, {ModuleBin, all}),
            apply_import_to_interpreters(ModuleBin),
            ok
    end.

%% @doc Register a module/function for import in all interpreters.
%%
%% Imports the module immediately in all running interpreters and adds it
%% to the registry for future interpreters.
%%
%% The `__main__' module is never cached.
%%
%% Example:
%% ```
%% ok = py_import:ensure_imported(json, dumps),
%% {ok, Result} = py:call(json, dumps, [Data]).
%% '''
%%
%% @param Module Python module name
%% @param Func Function name to register
%% @returns ok | {error, Reason}
-spec ensure_imported(py_module(), py_func()) -> ok | {error, term()}.
ensure_imported(Module, Func) ->
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    case ModuleBin of
        <<"__main__">> ->
            {error, main_not_cacheable};
        _ ->
            ets:insert(?IMPORT_REGISTRY, {ModuleBin, FuncBin}),
            apply_import_to_interpreters(ModuleBin),
            ok
    end.

%% @doc Check if a module is registered in the import registry.
%%
%% @param Module Python module name
%% @returns true if module is registered, false otherwise
-spec is_imported(py_module()) -> boolean().
is_imported(Module) ->
    ModuleBin = ensure_binary(Module),
    case ets:info(?IMPORT_REGISTRY) of
        undefined -> false;
        _ -> ets:member(?IMPORT_REGISTRY, ModuleBin)
    end.

%% @doc Check if a module/function is registered in the import registry.
%%
%% @param Module Python module name
%% @param Func Function name
%% @returns true if module/function is registered, false otherwise
-spec is_imported(py_module(), py_func()) -> boolean().
is_imported(Module, Func) ->
    ModuleBin = ensure_binary(Module),
    FuncBin = ensure_binary(Func),
    case ets:info(?IMPORT_REGISTRY) of
        undefined -> false;
        _ ->
            case ets:lookup(?IMPORT_REGISTRY, ModuleBin) of
                [{_, all}] -> true;
                [{_, FuncBin}] -> true;
                _ -> false
            end
    end.

%% @doc Get all registered imports from the global registry.
%%
%% Returns a list of {Module, Func | all} tuples representing all
%% modules/functions registered for automatic import.
%%
%% Example:
%% ```
%% ok = py_import:ensure_imported(json),
%% ok = py_import:ensure_imported(math, sqrt),
%% [{<<"json">>, all}, {<<"math">>, <<"sqrt">>}] = py_import:all_imports().
%% '''
%%
%% @returns List of {Module, Func | all} tuples
-spec all_imports() -> [{binary(), binary() | all}].
all_imports() ->
    case ets:info(?IMPORT_REGISTRY) of
        undefined -> [];
        _ -> ets:tab2list(?IMPORT_REGISTRY)
    end.

%% @doc Clear all registered imports from the global registry.
%%
%% Removes all entries from the registry.
%% Does not affect already-running interpreters.
%%
%% @returns ok
-spec clear_imports() -> ok.
clear_imports() ->
    case ets:info(?IMPORT_REGISTRY) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?IMPORT_REGISTRY)
    end,
    ok.

%% @doc List all registered imports.
%%
%% Returns a map of modules to their registered functions.
%% Module names are binary keys, function lists are the values.
%% An empty list means only the module is registered (no specific functions).
%%
%% Example:
%% ```
%% ok = py_import:ensure_imported(json),
%% ok = py_import:ensure_imported(json, dumps),
%% ok = py_import:ensure_imported(json, loads),
%% ok = py_import:ensure_imported(math),
%% {ok, #{<<"json">> => [<<"dumps">>, <<"loads">>],
%%        <<"math">> => []}} = py_import:import_list().
%% '''
%%
%% @returns {ok, #{Module => [Func]}} map of modules to functions
-spec import_list() -> {ok, #{binary() => [binary()]}} | {error, term()}.
import_list() ->
    Imports = all_imports(),
    %% Group by module
    Map = lists:foldl(fun({Module, FuncOrAll}, Acc) ->
        Existing = maps:get(Module, Acc, []),
        case FuncOrAll of
            all ->
                %% Module-level import, don't add to function list
                maps:put(Module, Existing, Acc);
            Func ->
                %% Function-level import
                maps:put(Module, [Func | Existing], Acc)
        end
    end, #{}, Imports),
    {ok, Map}.

%%% ============================================================================
%%% Path Registry (sys.path additions)
%%% ============================================================================

%% @doc Add a path to sys.path in all interpreters.
%%
%% Adds the path immediately to sys.path in all running interpreters
%% (contexts and event loops) and stores it for future interpreters.
%%
%% Example:
%% ```
%% ok = py_import:add_path("/path/to/my/modules"),
%% {ok, Result} = py:call(mymodule, myfunc, []).
%% '''
%%
%% @param Path Directory path to add (string, binary, or atom)
%% @returns ok
-spec add_path(string() | binary() | atom()) -> ok.
add_path(Path) ->
    PathBin = ensure_binary(Path),
    Key = erlang:monotonic_time(),
    ets:insert(?PATH_REGISTRY, {Key, PathBin}),
    apply_path_to_interpreters(PathBin),
    ok.

%% @doc Add multiple paths to sys.path in all interpreters.
%%
%% Adds all paths to the global path registry. Paths are added in order,
%% so the first path in the list will be first in sys.path.
%%
%% Example:
%% ```
%% ok = py_import:add_paths(["/path/to/lib1", "/path/to/lib2"]),
%% {ok, Result} = py:call(mymodule, myfunc, []).
%% '''
%%
%% @param Paths List of directory paths to add
%% @returns ok
-spec add_paths([string() | binary() | atom()]) -> ok.
add_paths(Paths) when is_list(Paths) ->
    lists:foreach(fun add_path/1, Paths),
    ok.

%% @doc Get all registered paths from the global registry.
%%
%% Returns a list of paths in the order they were added.
%%
%% Example:
%% ```
%% ok = py_import:add_path("/path/to/modules"),
%% [<<"/path/to/modules">>] = py_import:all_paths().
%% '''
%%
%% @returns List of paths as binaries
-spec all_paths() -> [binary()].
all_paths() ->
    case ets:info(?PATH_REGISTRY) of
        undefined -> [];
        _ ->
            %% ordered_set returns in key order (monotonic time = insertion order)
            [Path || {_Key, Path} <- ets:tab2list(?PATH_REGISTRY)]
    end.

%% @doc Clear all registered paths from the global registry.
%%
%% Removes all entries from the path registry.
%% Does not affect already-running interpreters.
%%
%% @returns ok
-spec clear_paths() -> ok.
clear_paths() ->
    case ets:info(?PATH_REGISTRY) of
        undefined -> ok;
        _ -> ets:delete_all_objects(?PATH_REGISTRY)
    end,
    ok.

%% @doc Check if a path is registered in the path registry.
%%
%% @param Path Directory path to check
%% @returns true if path is registered, false otherwise
-spec is_path_added(string() | binary() | atom()) -> boolean().
is_path_added(Path) ->
    PathBin = ensure_binary(Path),
    case ets:info(?PATH_REGISTRY) of
        undefined -> false;
        _ ->
            case ets:match(?PATH_REGISTRY, {'_', PathBin}) of
                [] -> false;
                _ -> true
            end
    end.

%%% ============================================================================
%%% Internal functions
%%% ============================================================================

%% @private
ensure_binary(S) ->
    py_util:to_binary(S).

%% @private Load imports and paths from application config
load_config() ->
    %% Load imports: [{Module, Func}]
    Imports = application:get_env(erlang_python, imports, []),
    lists:foreach(
        fun({Module, Func}) ->
            ModuleBin = ensure_binary(Module),
            FuncBin = ensure_binary(Func),
            ets:insert(?IMPORT_REGISTRY, {ModuleBin, FuncBin})
        end,
        Imports
    ),
    %% Load paths
    Paths = application:get_env(erlang_python, paths, []),
    lists:foreach(
        fun(Path) ->
            PathBin = ensure_binary(Path),
            Key = erlang:monotonic_time(),
            ets:insert(?PATH_REGISTRY, {Key, PathBin})
        end,
        Paths
    ),
    ok.

%% @private Apply import to all running interpreters (contexts + event loops)
apply_import_to_interpreters(ModuleBin) ->
    Imports = [{ModuleBin, all}],
    %% Apply to all contexts
    lists:foreach(
        fun(Ctx) ->
            try
                Ref = py_context:get_nif_ref(Ctx),
                py_nif:interp_apply_imports(Ref, Imports)
            catch _:_ -> ok
            end
        end,
        get_all_contexts()
    ),
    %% Apply to main event loop
    case py_event_loop:get_loop() of
        {ok, LoopRef} ->
            catch py_nif:interp_apply_imports(LoopRef, Imports);
        _ -> ok
    end,
    %% Apply to all pool event loops
    case py_event_loop_pool:get_all_loops() of
        {ok, Loops} ->
            lists:foreach(
                fun({LoopRef, _WorkerPid}) ->
                    catch py_nif:interp_apply_imports(LoopRef, Imports)
                end,
                Loops
            );
        _ -> ok
    end,
    ok.

%% @private Apply path to all running interpreters (contexts + event loops)
apply_path_to_interpreters(PathBin) ->
    Paths = [PathBin],
    %% Apply to all contexts
    lists:foreach(
        fun(Ctx) ->
            try
                Ref = py_context:get_nif_ref(Ctx),
                py_nif:interp_apply_paths(Ref, Paths)
            catch _:_ -> ok
            end
        end,
        get_all_contexts()
    ),
    %% Apply to main event loop
    case py_event_loop:get_loop() of
        {ok, LoopRef} ->
            catch py_nif:interp_apply_paths(LoopRef, Paths);
        _ -> ok
    end,
    %% Apply to all pool event loops
    case py_event_loop_pool:get_all_loops() of
        {ok, Loops} ->
            lists:foreach(
                fun({LoopRef, _WorkerPid}) ->
                    catch py_nif:interp_apply_paths(LoopRef, Paths)
                end,
                Loops
            );
        _ -> ok
    end,
    ok.

%% @private Get all context pids from all pools
get_all_contexts() ->
    DefaultCtxs = try py_context_router:contexts() catch _:_ -> [] end,
    %% Could add other pools here if needed
    DefaultCtxs.
