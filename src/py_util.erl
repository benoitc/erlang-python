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
    escape_py_literal/1,
    valid_py_module/1,
    valid_py_ident/1
]).

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

%% @doc Escape a binary for use inside a single-quoted Python string literal.
-spec escape_py_literal(binary()) -> binary().
escape_py_literal(Bin) when is_binary(Bin) ->
    << <<(escape_py_byte(B))/binary>> || <<B>> <= Bin >>.

escape_py_byte($') -> <<"\\'">>;
escape_py_byte($\\) -> <<"\\\\">>;
escape_py_byte($\n) -> <<"\\n">>;
escape_py_byte($\r) -> <<"\\r">>;
escape_py_byte($\t) -> <<"\\t">>;
escape_py_byte(B) when B < 16#20; B =:= 16#7f ->
    list_to_binary(io_lib:format("\\x~2.16.0b", [B]));
escape_py_byte(B) -> <<B>>.

%% @private Validate a dotted Python module path (each segment an identifier).
valid_py_module(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    Segments = binary:split(Bin, <<".">>, [global]),
    lists:foreach(fun valid_py_ident/1, Segments),
    Bin;
valid_py_module(Other) ->
    error({invalid_python_identifier, Other}).

ident_ok(<<>>, first) -> false;   %% empty segment (leading/trailing/double dot)
ident_ok(<<>>, rest) -> true;
ident_ok(<<C, Rest/binary>>, first)
  when (C >= $A andalso C =< $Z); (C >= $a andalso C =< $z); C =:= $_ ->
    ident_ok(Rest, rest);
ident_ok(<<C, Rest/binary>>, rest)
  when (C >= $A andalso C =< $Z); (C >= $a andalso C =< $z);
       (C >= $0 andalso C =< $9); C =:= $_ ->
    ident_ok(Rest, rest);
ident_ok(_, _) -> false.

%% @private Validate a Python identifier ([A-Za-z_][A-Za-z0-9_]*). Crashes on a
%% non-conforming value so an attacker-controlled module/func/kwarg name can't
%% inject code at an identifier position (where quoting is meaningless).
valid_py_ident(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    case ident_ok(Bin, first) of
        true -> Bin;
        false -> error({invalid_python_identifier, Bin})
    end;
valid_py_ident(Other) ->
    error({invalid_python_identifier, Other}).
