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

%%% @doc Python preload code registry.
%%%
%%% Allows users to preload Python code that executes during interpreter
%%% initialization. The resulting globals become the base namespace that
%%% process-local environments inherit from.
%%%
%%% == Usage ==
%%%
%%% ```
%%% %% At application startup
%%% py_preload:set_code(<<"
%%% import json
%%% import os
%%%
%%% def shared_helper(x):
%%%     return x * 2
%%%
%%% CONFIG = {'debug': True}
%%% ">>).
%%%
%%% %% Later, any context will have these preloaded
%%% {ok, Ctx} = py_context:new(#{mode => worker}),
%%% {ok, 10} = py:eval(Ctx, <<"shared_helper(5)">>).
%%% '''
%%%
%%% == Storage ==
%%%
%%% Uses `persistent_term' for the preload code. Changes only affect
%%% newly created contexts; existing contexts are not modified.
%%%
%%% @end
-module(py_preload).

-export([
    set_code/1,
    get_code/0,
    clear_code/0,
    has_preload/0,
    apply_preload/1
]).

-define(PRELOAD_KEY, {py_preload, code}).

%% @doc Set preload code to be executed once per interpreter at init.
%%
%% The code is executed in the interpreter's `__main__' namespace.
%% All defined functions, variables, and imports become available
%% in process-local environments.
%%
%% @param Code Python code as binary or iolist
-spec set_code(binary() | iolist()) -> ok.
set_code(Code) when is_binary(Code); is_list(Code) ->
    persistent_term:put(?PRELOAD_KEY, iolist_to_binary(Code)).

%% @doc Get the current preload code.
%%
%% @returns The preload code binary, or `undefined' if not set
-spec get_code() -> binary() | undefined.
get_code() ->
    try
        persistent_term:get(?PRELOAD_KEY)
    catch
        error:badarg -> undefined
    end.

%% @doc Clear the preload code.
%%
%% New contexts will start with empty globals. Existing contexts
%% are not affected.
-spec clear_code() -> ok.
clear_code() ->
    try
        persistent_term:erase(?PRELOAD_KEY)
    catch
        error:badarg -> ok
    end,
    ok.

%% @doc Check if preload code is configured.
-spec has_preload() -> boolean().
has_preload() ->
    get_code() =/= undefined.

%% @doc Apply preload code to a context reference.
%%
%% Called internally by `py_context' during context initialization.
%% Executes the preload code in the context's interpreter.
%%
%% @param Ref NIF context reference
%% @returns `ok' if successful or no preload configured, `{error, Reason}' on failure
-spec apply_preload(reference()) -> ok | {error, term()}.
apply_preload(Ref) ->
    case get_code() of
        undefined -> ok;
        Code -> py_nif:context_exec(Ref, Code)
    end.
