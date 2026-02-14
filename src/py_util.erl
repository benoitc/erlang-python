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

%%% @doc Common utility functions for the py application.
%%%
%%% @private
-module(py_util).

-export([
    to_binary/1,
    send_response/3,
    normalize_timeout/1,
    normalize_timeout/2
]).

%% Default timeout (30 seconds)
-define(DEFAULT_TIMEOUT, 30000).

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Convert atom, list, or binary to binary.
-spec to_binary(atom() | list() | binary()) -> binary().
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.

%% @doc Send a response to a caller.
-spec send_response(pid(), reference(), {ok, term()} | {error, term()} | ok) -> ok.
send_response(Caller, Ref, {ok, Value}) ->
    Caller ! {py_response, Ref, {ok, Value}},
    ok;
send_response(Caller, Ref, {error, Error}) ->
    Caller ! {py_error, Ref, Error},
    ok;
send_response(Caller, Ref, ok) ->
    Caller ! {py_response, Ref, {ok, none}},
    ok.

%% @doc Normalize a timeout value, returning milliseconds or 0 for infinity.
%% Uses the default timeout for invalid values.
-spec normalize_timeout(timeout()) -> non_neg_integer().
normalize_timeout(Timeout) ->
    normalize_timeout(Timeout, ?DEFAULT_TIMEOUT).

%% @doc Normalize a timeout value with a custom default.
-spec normalize_timeout(timeout(), non_neg_integer()) -> non_neg_integer().
normalize_timeout(infinity, _Default) -> 0;
normalize_timeout(Ms, _Default) when is_integer(Ms), Ms > 0 -> Ms;
normalize_timeout(_, Default) -> Default.
