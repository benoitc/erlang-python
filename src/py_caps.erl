%% Copyright 2026 Benoit Chesneau
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%     http://www.apache.org/licenses/LICENSE-2.0
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc The `caps' option: what an isolated child may reach.
%%%
%%% A capability grant names directories, environment variables and network
%%% addresses. Anything not named is not reachable. This module reads the
%%% option, refuses what it cannot make sense of, and renders the result as
%%% the JSON the child parses before it runs any user code.
%%%
%%% ```
%%% #{dirs => [{"/srv/models", read}, {"/var/data/job42", write}],
%%%   env  => #{<<"MODEL_DIR">> => <<"/srv/models">>},
%%%   net  => #{connect => [{tcp, <<"10.0.0.0/8">>, {5432, 5432}}],
%%%             listen  => [{tcp, <<"127.0.0.1">>, 8080}],
%%%             resolve => deny}}
%%% '''
%%%
%%% A rule is `{Proto, Addr, Port}': `Proto' is `tcp' or `udp', `Addr' is an
%%% address tuple, a binary address or a binary CIDR, and `Port' is an integer,
%%% `{Lo, Hi}' or `any'. Rules name addresses and never host names: a name
%%% would have to be resolved to be checked and resolved again to be used, and
%%% the two answers can differ. Resolution is its own capability and what it
%%% returns carries no authority.
%%%
%%% Checked here rather than in the child, so a typo is a `{bad_caps, _}'
%%% error from `py_context:new/1' rather than a connection refused much later.
%%% A sandbox that silently refuses everything looks exactly like one that
%%% works.
%%%
%%% The rule shape, the IPv4-mapped folding and the masking are taken from
%%% `wasi_net.erl' in erlang_wasm, so a grant means the same thing in both.
%%%
%%% @private
%%%
%%% Shared memory is not granted here and does not work under a capability
%%% set: a region arrives as a path, and granting the directory holding them
%%% would hand over every region this node owns. The way to make it work is
%%% to pass the region's descriptor rather than its name; see
%%% `docs/capabilities.md'.
%%%
%%% Owns: the meaning of the `caps' option and its wire form.
%%% Talks to: `py_context' (validation at `new/1'), `py_isolated' (argv).
%%% Never: enforces anything; the child does that, in
%%%   `priv/_erlang_impl/_caps.py'.
%%% @end
-module(py_caps).

-export([
    validate/1,
    to_json/1
]).

-export_type([caps/0, access/0]).

-type access() :: read | write.
-type rule() :: {tcp | udp, {inet:ip_address(), 0..128}, {0..65535, 0..65535}}.
-type net() :: none | #{connect := [rule()], listen := [rule()],
                        resolve := boolean()}.
-type caps() :: #{dirs := [{binary(), access()}],
                  env := #{binary() => binary()},
                  net := net()}.

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Read a `caps' option into the form the child is given.
%%
%% `{error, {bad_caps, Detail}}' names the part that could not be read.
-spec validate(term()) -> {ok, caps()} | {error, {bad_caps, term()}}.
validate(Map) when is_map(Map) ->
    try
        Known = [dirs, env, net],
        case maps:keys(maps:without(Known, Map)) of
            [] -> ok;
            Extra -> throw({unknown_keys, Extra})
        end,
        {ok, #{dirs => dirs(maps:get(dirs, Map, [])),
               env => env(maps:get(env, Map, #{})),
               net => net(maps:get(net, Map, none))}}
    catch
        throw:Detail -> {error, {bad_caps, Detail}}
    end;
validate(Other) ->
    {error, {bad_caps, Other}}.

%% @doc Render a validated grant as the JSON passed to the child in argv.
-spec to_json(caps()) -> binary().
to_json(#{dirs := Dirs, env := Env, net := Net}) ->
    iolist_to_binary(json:encode(
        #{<<"dirs">> => [#{<<"path">> => P, <<"access">> => atom_to_binary(A)}
                         || {P, A} <- Dirs],
          <<"env">> => Env,
          <<"net">> => net_json(Net)})).

%%% ============================================================================
%%% Directories and environment
%%% ============================================================================

dirs(L) when is_list(L) ->
    [dir(D) || D <- L];
dirs(Other) ->
    throw({dirs, Other}).

%% An absolute path, so that what a grant covers does not depend on the
%% working directory of whoever wrote it.
dir({Path, Access}) when Access =:= read; Access =:= write ->
    case to_bin(Path) of
        <<"/", _/binary>> = Bin -> {Bin, Access};
        _ -> throw({dir_not_absolute, Path})
    end;
dir(Other) ->
    throw({dir, Other}).

env(Map) when is_map(Map) ->
    maps:from_list([{to_bin(K), to_bin(V)} || {K, V} <- maps:to_list(Map)]);
env(Other) ->
    throw({env, Other}).

%%% ============================================================================
%%% Network grant
%%%
%%% From wasi_net.erl (erlang_wasm), which parses the same rules for a WASM
%%% guest. Kept in step with it deliberately: a grant should mean one thing.
%%% ============================================================================

net(none) -> none;
net(undefined) -> none;
net(Map) when is_map(Map) ->
    case maps:keys(maps:without([connect, listen, resolve], Map)) of
        [] -> ok;
        Extra -> throw({net, {unknown_keys, Extra}})
    end,
    #{connect => rules(maps:get(connect, Map, [])),
      listen => rules(maps:get(listen, Map, [])),
      resolve => resolve(maps:get(resolve, Map, deny))};
net(Other) ->
    throw({net, Other}).

resolve(allow) -> true;
resolve(deny) -> false;
resolve(Other) -> throw({net, {resolve, Other}}).

rules(L) when is_list(L) -> [rule(R) || R <- L];
rules(Other) -> throw({net, Other}).

rule({Proto, Addr, Port}) when Proto =:= tcp; Proto =:= udp ->
    {Proto, cidr(Addr), ports(Port)};
rule(Other) ->
    throw({net, {rule, Other}}).

%% An address with no prefix length is one host: a full-width prefix.
cidr(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<"/">>) of
        [Addr] -> host(parse_or_fail(Addr));
        [Addr, Len] -> network(parse_or_fail(Addr), integer_or_fail(Len, Bin), Bin)
    end;
cidr(Tuple) when tuple_size(Tuple) =:= 4; tuple_size(Tuple) =:= 8 ->
    host(Tuple);
cidr(Other) ->
    throw({net, {address, Other}}).

host(Addr0) ->
    Addr = normalise(Addr0),
    {Addr, width(Addr)}.

%% The prefix length is written in the notation the address was written in, so
%% a mapped base has to have its 96 mapping bits taken off with it. Below 96
%% the prefix spans addresses inside and outside the mapped block at once,
%% which has no IPv4 meaning; refuse rather than guess which half was meant.
network(Addr, Bits, Written) ->
    case normalise(Addr) of
        A when tuple_size(A) =:= 4, Bits >= 0, Bits =< 32 ->
            {mask(A, Bits), Bits};
        A when tuple_size(A) =:= 8, Bits >= 0, Bits =< 128 ->
            {mask(A, Bits), Bits};
        V4 when Bits >= 96, Bits =< 128 ->
            {mask(V4, Bits - 96), Bits - 96};
        _ ->
            throw({net, {address, Written}})
    end.

ports(any) -> {0, 65535};
ports(P) when is_integer(P), P >= 0, P =< 65535 -> {P, P};
ports({Lo, Hi}) when is_integer(Lo), is_integer(Hi), Lo >= 0, Lo =< Hi,
                     Hi =< 65535 -> {Lo, Hi};
ports(Other) -> throw({net, {port, Other}}).

parse_or_fail(Bin) ->
    case inet:parse_address(binary_to_list(Bin)) of
        {ok, Addr} -> normalise(Addr);
        {error, _} -> throw({net, {address, Bin}})
    end.

integer_or_fail(Bin, Written) ->
    try binary_to_integer(Bin)
    catch _:_ -> throw({net, {address, Written}})
    end.

%% Fold an IPv4-mapped IPv6 address onto the IPv4 address it reaches, so
%% `::ffff:127.0.0.1' cannot walk past a `127.0.0.0/8' rule. The deprecated
%% IPv4-compatible block is left alone: `::0.0.0.1' and `::1' are the same
%% address, so folding it would make loopback ambiguous.
normalise({0, 0, 0, 0, 0, 16#ffff, X, Y}) ->
    {X bsr 8, X band 16#ff, Y bsr 8, Y band 16#ff};
normalise(Addr) ->
    Addr.

width(Addr) when tuple_size(Addr) =:= 4 -> 32;
width(Addr) when tuple_size(Addr) =:= 8 -> 128.

%% Zeroing the host bits, so a rule written `10.1.2.3/8' means the same
%% network as `10.0.0.0/8' rather than never matching anything.
mask(Addr, Bits) ->
    W = width(Addr),
    from_int(to_int(Addr) band (((1 bsl Bits) - 1) bsl (W - Bits)), W).

to_int(Addr) ->
    Size = part_size(Addr),
    lists:foldl(fun(P, Acc) -> (Acc bsl Size) bor P end, 0, tuple_to_list(Addr)).

from_int(N, 32) ->
    <<A, B, C, D>> = <<N:32>>,
    {A, B, C, D};
from_int(N, 128) ->
    <<A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>> = <<N:128>>,
    {A, B, C, D, E, F, G, H}.

part_size(Addr) when tuple_size(Addr) =:= 4 -> 8;
part_size(Addr) when tuple_size(Addr) =:= 8 -> 16.

%%% ============================================================================
%%% Wire form
%%% ============================================================================

net_json(none) ->
    null;
net_json(#{connect := C, listen := L, resolve := R}) ->
    #{<<"connect">> => [rule_json(Rule) || Rule <- C],
      <<"listen">> => [rule_json(Rule) || Rule <- L],
      <<"resolve">> => R}.

%% The child matches with Python's `ipaddress', so rules cross as the CIDR
%% text that module reads. The address is already masked and folded here, so
%% both sides agree on what a rule covers without parsing it twice.
rule_json({Proto, {Addr, Bits}, {Lo, Hi}}) ->
    #{<<"proto">> => atom_to_binary(Proto),
      <<"cidr">> => iolist_to_binary([inet:ntoa(Addr), "/", integer_to_list(Bits)]),
      <<"ports">> => [Lo, Hi]}.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(Other) -> throw({not_a_string, Other}).
