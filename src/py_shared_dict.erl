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

%%% @doc Process-scoped shared dictionaries (`py:shared_dict_*').
%%% Thin wrappers over the `shared_dict_*' NIFs; use the `py' functions.
%%% @private
-module(py_shared_dict).

-export([
    shared_dict_new/0,
    shared_dict_get/2,
    shared_dict_get/3,
    shared_dict_set/3,
    shared_dict_del/2,
    shared_dict_keys/1,
    shared_dict_destroy/1
]).


%% @doc Create a new process-scoped SharedDict.
%%
%% Creates a SharedDict owned by the calling process. The dict is automatically
%% destroyed when the owning process terminates. Values are stored as pickled
%% bytes for cross-interpreter safety.
%%
%% == Example ==
%% ```
%% {ok, SD} = py:shared_dict_new().
%% ok = py:shared_dict_set(SD, <<"config">>, #{host => <<"localhost">>}).
%% #{<<"host">> := <<"localhost">>} = py:shared_dict_get(SD, <<"config">>).
%% '''
%%
%% @returns {ok, Reference} on success, {error, Reason} on failure
-spec shared_dict_new() -> {ok, reference()} | {error, term()}.
shared_dict_new() ->
    py_nif:shared_dict_new().

%% @doc Get a value from SharedDict with default undefined.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @returns Value or undefined if key not found
-spec shared_dict_get(reference(), binary()) -> term().
shared_dict_get(Handle, Key) ->
    shared_dict_get(Handle, Key, undefined).

%% @doc Get a value from SharedDict with custom default.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Default Default value if key not found
%% @returns Value or Default
-spec shared_dict_get(reference(), binary(), term()) -> term().
shared_dict_get(Handle, Key, Default) when is_binary(Key) ->
    py_nif:shared_dict_get(Handle, Key, Default).

%% @doc Set a value in SharedDict.
%%
%% The value is pickled for cross-interpreter safety.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @param Value Erlang term value (will be pickled)
%% @returns ok on success
-spec shared_dict_set(reference(), binary(), term()) -> ok | {error, term()}.
shared_dict_set(Handle, Key, Value) when is_binary(Key) ->
    py_nif:shared_dict_set(Handle, Key, Value).

%% @doc Delete a key from SharedDict.
%%
%% @param Handle SharedDict reference
%% @param Key Binary key
%% @returns ok (even if key didn't exist)
-spec shared_dict_del(reference(), binary()) -> ok.
shared_dict_del(Handle, Key) when is_binary(Key) ->
    py_nif:shared_dict_del(Handle, Key).

%% @doc Get all keys from SharedDict.
%%
%% @param Handle SharedDict reference
%% @returns List of binary keys
-spec shared_dict_keys(reference()) -> [binary()].
shared_dict_keys(Handle) ->
    py_nif:shared_dict_keys(Handle).

%% @doc Explicitly destroy a SharedDict.
%%
%% Marks the SharedDict as destroyed and clears its Python dict.
%% After destruction, any further operations on this SharedDict will
%% return badarg. This is idempotent - calling on an already-destroyed
%% dict returns ok.
%%
%% @param Handle SharedDict reference
%% @returns ok
-spec shared_dict_destroy(reference()) -> ok.
shared_dict_destroy(Handle) ->
    py_nif:shared_dict_destroy(Handle).


