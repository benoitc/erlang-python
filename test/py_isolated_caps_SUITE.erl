%% @doc The `caps' option: what an isolated child may reach.
%%
%% The path cases are the ones that matter. A capability set that opens the
%% right files is easy; one that reliably refuses the wrong ones is the whole
%% point. Each escape technique gets its own case, and each is refused as a
%% capability error rather than as a missing file, so the error cannot be used
%% to find out what exists outside a grant.
%%
%% The case names follow `wasi_SUITE' and `wasi_net_SUITE' in erlang_wasm,
%% whose grant model this implements, so the two can be read side by side.
-module(py_isolated_caps_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    reads_inside_a_grant/1,
    parent_traversal_is_refused/1,
    absolute_path_is_refused/1,
    partial_traversal_is_refused/1,
    symlink_escape_is_refused/1,
    symlink_directory_prefix_is_refused/1,
    a_symlink_inside_a_grant_is_followed/1,
    a_symlink_cycle_is_refused_rather_than_followed/1,
    missing_file_inside_a_grant_is_not_a_refusal/1,
    a_read_grant_yields_no_write_whatever_the_flags/1,
    a_write_grant_allows_create_and_unlink/1,
    listing_is_granted_with_the_directory/1,
    imports_still_work/1,
    a_network_and_a_port_range/1,
    an_ungranted_port_is_refused_even_where_something_listens/1,
    binding_is_checked_against_listen_not_connect/1,
    resolution_is_its_own_capability/1,
    resolution_cannot_widen_a_grant/1,
    a_wildcard_grant_really_is_a_wildcard/1,
    no_net_key_is_no_network/1,
    a_passed_fd_still_serves/1,
    env_is_what_was_granted_and_nothing_else/1,
    subprocess_is_refused/1,
    ctypes_is_refused/1,
    the_policy_cannot_be_switched_off_from_python/1,
    signalling_another_process_is_refused/1,
    unaudited_creators_are_taken_away/1,
    every_resolver_is_gated_not_only_getaddrinfo/1,
    the_env_option_cannot_widen_a_grant/1,
    shared_memory_is_refused_under_a_capability_set/1,
    a_user_fspath_runs_enforced/1,
    a_unix_socket_is_not_a_file/1,
    a_read_grant_cannot_become_a_write/1,
    caps_survive_a_child_restart/1,
    child_info_reports_the_grants/1,
    caps_are_rejected_outside_isolated/1,
    a_malformed_rule_is_a_configuration_error/1,
    no_caps_changes_nothing/1
]).

-define(TEST_MOD, py_test_caps).

all() ->
    [
        %% filesystem
        reads_inside_a_grant,
        parent_traversal_is_refused,
        absolute_path_is_refused,
        partial_traversal_is_refused,
        symlink_escape_is_refused,
        symlink_directory_prefix_is_refused,
        a_symlink_inside_a_grant_is_followed,
        a_symlink_cycle_is_refused_rather_than_followed,
        missing_file_inside_a_grant_is_not_a_refusal,
        a_read_grant_yields_no_write_whatever_the_flags,
        a_write_grant_allows_create_and_unlink,
        listing_is_granted_with_the_directory,
        imports_still_work,
        %% network
        a_network_and_a_port_range,
        an_ungranted_port_is_refused_even_where_something_listens,
        binding_is_checked_against_listen_not_connect,
        resolution_is_its_own_capability,
        resolution_cannot_widen_a_grant,
        a_wildcard_grant_really_is_a_wildcard,
        no_net_key_is_no_network,
        a_passed_fd_still_serves,
        %% the rest
        env_is_what_was_granted_and_nothing_else,
        subprocess_is_refused,
        ctypes_is_refused,
        the_policy_cannot_be_switched_off_from_python,
        signalling_another_process_is_refused,
        unaudited_creators_are_taken_away,
        every_resolver_is_gated_not_only_getaddrinfo,
        the_env_option_cannot_widen_a_grant,
        shared_memory_is_refused_under_a_capability_set,
        a_user_fspath_runs_enforced,
        a_unix_socket_is_not_a_file,
        a_read_grant_cannot_become_a_write,
        caps_survive_a_child_restart,
        child_info_reports_the_grants,
        caps_are_rejected_outside_isolated,
        a_malformed_rule_is_a_configuration_error,
        no_caps_changes_nothing
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config].

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

%% A tree with everything the escape cases need, made fresh for each case so
%% one case cannot leave a symlink behind for the next.
%%
%%   root/data/note.txt        readable
%%   root/data/sub/deep.txt    readable, one level down
%%   root/data/escape          -> root/secret/key.txt
%%   root/data/outdir          -> root/secret
%%   root/data/here            -> note.txt        (stays inside)
%%   root/data/loop            -> loop
%%   root/secret/key.txt       never granted
init_per_testcase(TestCase, Config) ->
    Root = filename:join(?config(priv_dir, Config), atom_to_list(TestCase)),
    Data = filename:join(Root, "data"),
    Secret = filename:join(Root, "secret"),
    ok = filelib:ensure_path(filename:join(Data, "sub")),
    ok = filelib:ensure_path(Secret),
    ok = file:write_file(filename:join(Data, "note.txt"), <<"inside">>),
    ok = file:write_file(filename:join([Data, "sub", "deep.txt"]), <<"deep">>),
    ok = file:write_file(filename:join(Secret, "key.txt"), <<"secret">>),
    ok = file:make_symlink(filename:join(Secret, "key.txt"),
                           filename:join(Data, "escape")),
    ok = file:make_symlink(Secret, filename:join(Data, "outdir")),
    ok = file:make_symlink("note.txt", filename:join(Data, "here")),
    ok = file:make_symlink("loop", filename:join(Data, "loop")),
    [{root, Root}, {data, Data}, {secret, Secret} | Config].

end_per_testcase(_TestCase, _Config) ->
    flush(),
    ok.

%%% ============================================================================
%%% Filesystem
%%% ============================================================================

reads_inside_a_grant(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, <<"inside">>} = read(C, path(Config, "note.txt")),
    {ok, <<"deep">>} = read(C, path(Config, "sub/deep.txt")),
    {ok, <<"inside">>} = read(C, path(Config, "./note.txt")),
    alive(C),
    stop(C).

parent_traversal_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, path(Config, "../secret/key.txt"))),
    alive(C),
    stop(C).

absolute_path_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, "/etc/hosts")),
    refused(read(C, filename:join(?config(secret, Config), "key.txt"))),
    alive(C),
    stop(C).

%% Leaves the grant and comes back. Refused because the path leaves at any
%% point, not merely because of where it ends up; and the half of it that is
%% legal really is legal, or this case would pass just as well with `..'
%% refused outright.
partial_traversal_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, path(Config, "sub/../../secret/key.txt"))),
    {ok, <<"inside">>} = read(C, path(Config, "sub/../note.txt")),
    alive(C),
    stop(C).

symlink_escape_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, path(Config, "escape"))),
    alive(C),
    stop(C).

symlink_directory_prefix_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, path(Config, "outdir/key.txt"))),
    alive(C),
    stop(C).

a_symlink_inside_a_grant_is_followed(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, <<"inside">>} = read(C, path(Config, "here")),
    alive(C),
    stop(C).

a_symlink_cycle_is_refused_rather_than_followed(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, path(Config, "loop"))),
    alive(C),
    stop(C).

%% A file that is not there is not a capability answer. Distinguishing the two
%% is the whole reason refusals are not `FileNotFoundError'.
missing_file_inside_a_grant_is_not_a_refusal(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {error, {'FileNotFoundError', _}} = read(C, path(Config, "missing.txt")),
    alive(C),
    stop(C).

a_read_grant_yields_no_write_whatever_the_flags(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(call(C, write_file, [path(Config, "new.txt"), <<"x">>])),
    refused(call(C, append_file, [path(Config, "note.txt"), <<"x">>])),
    refused(call(C, truncate_file, [path(Config, "note.txt")])),
    refused(call(C, remove_file, [path(Config, "note.txt")])),
    {ok, <<"inside">>} = file:read_file(path(Config, "note.txt")),
    alive(C),
    stop(C).

a_write_grant_allows_create_and_unlink(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), write}]}),
    {ok, <<"ok">>} = call(C, write_file, [path(Config, "new.txt"), <<"written">>]),
    {ok, <<"written">>} = file:read_file(path(Config, "new.txt")),
    {ok, <<"ok">>} = call(C, remove_file, [path(Config, "new.txt")]),
    false = filelib:is_regular(path(Config, "new.txt")),
    %% Still only this grant: the neighbouring directory is untouched.
    refused(call(C, write_file,
                 [filename:join(?config(secret, Config), "x"), <<"x">>])),
    alive(C),
    stop(C).

listing_is_granted_with_the_directory(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, Names} = call(C, list_dir, [data(Config)]),
    true = lists:member(<<"note.txt">>, Names),
    refused(call(C, list_dir, [?config(secret, Config)])),
    alive(C),
    stop(C).

%% The interpreter's own path is granted, or nothing would import at all.
imports_still_work(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, <<"[1, 2]">>} = py_context:eval(
        C, <<"__import__('json').dumps([1,2])">>),
    {ok, 4} = py_context:eval(C, <<"len(__import__('base64').b64encode(b'ab'))">>),
    stop(C).

%%% ============================================================================
%%% Network
%%% ============================================================================

a_network_and_a_port_range(Config) ->
    {LSock, Port} = listener(),
    C = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.0/8">>,
                                            {Port, Port}}]}}),
    {ok, <<"connected">>} = call(C, connect, [<<"127.0.0.1">>, Port]),
    {ok, _} = gen_tcp:accept(LSock, 2000),
    ok = gen_tcp:close(LSock),
    alive(C),
    stop(C).

%% Something is accepting on this port, and the answer is the same one a dead
%% port would get: nothing was attempted.
an_ungranted_port_is_refused_even_where_something_listens(Config) ->
    {LSock, Port} = listener(),
    Granted = free_port(),
    C = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.1">>, Granted}]}}),
    refused(call(C, connect, [<<"127.0.0.1">>, Port])),
    {error, timeout} = gen_tcp:accept(LSock, 300),
    ok = gen_tcp:close(LSock),
    alive(C),
    stop(C).

%% Binding claims a local address, which is what `listen' grants. A connect
%% grant for the same address does not carry it.
binding_is_checked_against_listen_not_connect(Config) ->
    Port = free_port(),
    C = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.1">>, any}],
                               listen => [{tcp, <<"127.0.0.1">>, Port}]}}),
    {ok, <<"bound">>} = call(C, bind, [<<"127.0.0.1">>, Port]),
    refused(call(C, bind, [<<"127.0.0.1">>, free_port()])),
    alive(C),
    stop(C).

resolution_is_its_own_capability(Config) ->
    C = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.1">>, any}]}}),
    refused(call(C, resolve, [<<"localhost">>])),
    stop(C),
    C2 = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.1">>, any}],
                                resolve => allow}}),
    {ok, _} = call(C2, resolve, [<<"localhost">>]),
    alive(C2),
    stop(C2).

%% An address learned by resolving carries no authority from having been
%% resolved: the connect is still checked, and still refused.
resolution_cannot_widen_a_grant(Config) ->
    {LSock, Port} = listener(),
    C = ctx(Config, #{net => #{connect => [{tcp, <<"10.0.0.0/8">>, any}],
                               resolve => allow}}),
    {ok, Addrs} = call(C, resolve, [<<"localhost">>]),
    true = lists:member(<<"127.0.0.1">>, Addrs),
    refused(call(C, connect, [<<"127.0.0.1">>, Port])),
    {error, timeout} = gen_tcp:accept(LSock, 300),
    ok = gen_tcp:close(LSock),
    stop(C).

%% An inverse case: the documented sharp edge is that nothing is denied
%% implicitly, so adding a hidden deny list has to break the build and force
%% the guide to be corrected.
a_wildcard_grant_really_is_a_wildcard(Config) ->
    {LSock, Port} = listener(),
    C = ctx(Config, #{net => #{connect => [{tcp, <<"0.0.0.0/0">>, any}]}}),
    {ok, <<"connected">>} = call(C, connect, [<<"127.0.0.1">>, Port]),
    {ok, _} = gen_tcp:accept(LSock, 2000),
    ok = gen_tcp:close(LSock),
    stop(C).

no_net_key_is_no_network(Config) ->
    {LSock, Port} = listener(),
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(call(C, connect, [<<"127.0.0.1">>, Port])),
    C2 = ctx(Config, #{net => #{}}),
    refused(call(C2, connect, [<<"127.0.0.1">>, Port])),
    {error, timeout} = gen_tcp:accept(LSock, 300),
    ok = gen_tcp:close(LSock),
    stop(C),
    stop(C2).

%% A socket Erlang opened and handed over needs no grant: the child was given
%% the descriptor, which is the capability.
a_passed_fd_still_serves(Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, {127,0,0,1}},
                                     {active, false}, {backlog, 8}]),
    {ok, Port} = inet:port(LSock),
    {ok, Fd} = inet:getfd(LSock),
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, ChildFd} = py_context:pass_fd(C, Fd),
    ok = py_context:start_loop(C),
    {ok, _} = py_context:submit_await(C, ?TEST_MOD, serve, [ChildFd], #{}, 10000),
    {ok, Sock} = gen_tcp:connect({127,0,0,1}, Port, [binary, {active, false}], 2000),
    ok = gen_tcp:send(Sock, <<"ping">>),
    {ok, <<"pong">>} = gen_tcp:recv(Sock, 4, 5000),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(LSock),
    ok = py_context:stop_loop(C),
    stop(C).

%%% ============================================================================
%%% Environment, processes, shared memory, lifecycle
%%% ============================================================================

env_is_what_was_granted_and_nothing_else(Config) ->
    true = os:putenv("EP_CAPS_SECRET", "leaked"),
    C = ctx(Config, #{env => #{<<"EP_CAPS_GRANTED">> => <<"yes">>}}),
    {ok, <<"yes">>} = call(C, getenv, [<<"EP_CAPS_GRANTED">>]),
    {ok, none} = call(C, getenv, [<<"EP_CAPS_SECRET">>]),
    {ok, none} = call(C, getenv, [<<"HOME">>]),
    stop(C),
    %% Without a capability set the child inherits as it always did.
    C2 = ctx(Config, no_caps),
    {ok, <<"leaked">>} = call(C2, getenv, [<<"EP_CAPS_SECRET">>]),
    stop(C2),
    true = os:unsetenv("EP_CAPS_SECRET").

subprocess_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(call(C, run_subprocess, [])),
    refused(py_context:eval(C, <<"__import__('os').fork()">>)),
    alive(C),
    stop(C).

ctypes_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(py_context:eval(C, <<"__import__('ctypes').CDLL(None)">>)),
    alive(C),
    stop(C).

%% The hook must not read anything the workload can assign to, or the policy
%% is off the moment code says so.
the_policy_cannot_be_switched_off_from_python(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, "/etc/hosts")),
    %% Every name the hook used to resolve when it ran, assigned at once.
    ok = py_context:exec(C, <<"import _erlang_impl._caps as c\n"
                              "c._summary = None\n"
                              "c._local = type('x', (), {'busy': True})()\n"
                              "c._writes = lambda *a: False\n"
                              "c._PATH_EVENTS = {}\n"
                              "c._RESOLVE_EVENTS = frozenset()\n"
                              "c._SUBPROCESS_EVENTS = frozenset()\n"
                              "c._make_enforcer = None\n"
                              "c.os = None\n">>),
    refused(read(C, "/etc/hosts")),
    refused(call(C, run_subprocess, [])),
    %% And the levers that used to let Python widen a grant are gone.
    {ok, false} = py_context:eval(
        C, <<"hasattr(__import__('_erlang_impl._caps', fromlist=['x']),"
             " 'allow_path')">>),
    {ok, false} = py_context:eval(
        C, <<"hasattr(__import__('_erlang_impl._caps', fromlist=['x']),"
             " '_walk')">>),
    alive(C),
    stop(C).

%% The child shares the node's user and its parent is the BEAM, so an
%% unchecked signal is a way to take the node down.
signalling_another_process_is_refused(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(py_context:eval(C, <<"__import__('os').kill(__import__('os')"
                                 ".getppid(), 0)">>)),
    refused(py_context:eval(C, <<"__import__('os').killpg(__import__('os')"
                                 ".getpgrp(), 0)">>)),
    refused(py_context:eval(C, <<"__import__('os').kill(1, 0)">>)),
    %% Signalling itself is its own business.
    {ok, none} = py_context:eval(C, <<"__import__('os').kill(__import__('os')"
                                      ".getpid(), 0)">>),
    alive(C),
    stop(C).

%% CPython raises no audit event for these, so they cannot be refused and
%% are taken away instead. Their absence is the assertion.
unaudited_creators_are_taken_away(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), write}]}),
    {ok, false} = py_context:eval(C, <<"hasattr(__import__('os'), 'mkfifo')">>),
    {ok, false} = py_context:eval(C, <<"hasattr(__import__('os'), 'mknod')">>),
    {ok, false} = py_context:eval(C, <<"hasattr(__import__('posix'), 'mkfifo')">>),
    {ok, false} = py_context:eval(C, <<"hasattr(__import__('posix'), 'mknod')">>),
    alive(C),
    stop(C).

%% Gating only getaddrinfo would leave every other resolver as a way out,
%% and a name lookup is a message to whoever answers it.
every_resolver_is_gated_not_only_getaddrinfo(Config) ->
    C = ctx(Config, #{net => #{connect => [{tcp, <<"127.0.0.1">>, any}]}}),
    refused(call(C, resolve, [<<"localhost">>])),
    refused(py_context:eval(C, <<"__import__('socket').gethostbyname('localhost')">>)),
    refused(py_context:eval(C, <<"__import__('socket').gethostbyname_ex('localhost')">>)),
    refused(py_context:eval(C, <<"__import__('socket').gethostbyaddr('127.0.0.1')">>)),
    refused(py_context:eval(C, <<"__import__('socket').getnameinfo(('127.0.0.1',80),0)">>)),
    refused(py_context:eval(C, <<"__import__('socket').gethostname()">>)),
    alive(C),
    stop(C).

%% The `env' option adds to the environment and a grant says what the whole
%% of it is; the port keeps the last setting for a name, so taking both
%% would let the option quietly win.
the_env_option_cannot_widen_a_grant(_Config) ->
    {error, {bad_caps, env_option_conflicts_with_caps_env}} =
        py_context:new(#{mode => isolated,
                         caps => #{env => #{<<"A">> => <<"1">>}},
                         env => #{<<"SECRET">> => <<"leaked">>}}),
    {error, {bad_caps, env_option_conflicts_with_caps_env}} =
        py_context:new(#{mode => isolated, caps => #{},
                         env => #{<<"SECRET">> => <<"leaked">>}}),
    ok.

%% Shared memory does not combine with a capability set yet: a region
%% arrives as a path, and the only way to grant it would be to hand over the
%% directory holding every region this node owns. Passing the descriptor is
%% the fix, and it is not here yet, so the refusal has to be legible.
shared_memory_is_refused_under_a_capability_set(Config) ->
    case py_shm:available() of
        false ->
            {skip, "iommap not available"};
        true ->
            C = ctx(Config, #{dirs => [{data(Config), read}]}),
            {ok, Shm} = py_shm:new(4096),
            refused(py_context:call(C, ?TEST_MOD, shm_write,
                                    [Shm, <<"payload">>], #{}, 10000)),
            %% Erlang still has it, unharmed.
            ok = py_shm:write(Shm, 0, <<"payload">>),
            {ok, <<"payload">>} = py_shm:read(Shm, 0, 7),
            alive(C),
            ok = py_shm:close(Shm),
            stop(C)
    end.

%% Path conversion happens before the re-entrancy guard, so a `__fspath__'
%% method is user code that runs with the hook live rather than a window in
%% which everything is allowed.
a_user_fspath_runs_enforced(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    {ok, false} = py_context:call(C, ?TEST_MOD, read_through_fspath,
                                  [path(Config, "note.txt")], #{}, 10000),
    alive(C),
    stop(C).

%% Reaching a Unix socket is talking to whatever is behind it, which a
%% directory grant says nothing about.
a_unix_socket_is_not_a_file(Config) ->
    %% Its own short directory: an AF_UNIX path is capped near 104 bytes and
    %% `connect' rejects a longer one before the hook ever sees it, which
    %% would make this case pass for the wrong reason.
    Dir = "/tmp/ep_caps_u" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_path(Dir),
    C = ctx(Config, #{dirs => [{Dir, write}]}),
    try
        %% The directory is granted for writing, so the path is reachable as
        %% a file; talking through it is not what that grant said.
        {ok, <<"ok">>} = call(C, write_file, [Dir ++ "/plain", <<"x">>]),
        refused(call(C, unix_connect, [Dir ++ "/sock"])),
        refused(call(C, unix_bind, [Dir ++ "/mine.sock"])),
        alive(C)
    after
        stop(C),
        _ = file:del_dir_r(Dir)
    end.

%% Every route from a read grant to a write, including the ones CPython
%% does not announce by path.
a_read_grant_cannot_become_a_write(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(call(C, write_file, [path(Config, "note.txt"), <<"x">>])),
    refused(call(C, truncate_by_descriptor, [path(Config, "note.txt")])),
    {ok, <<"inside">>} = file:read_file(path(Config, "note.txt")),
    alive(C),
    stop(C).

caps_survive_a_child_restart(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}]}),
    refused(read(C, "/etc/hosts")),
    ok = py_context:kill(C),
    {ok, <<"inside">>} = read(C, path(Config, "note.txt")),
    refused(read(C, "/etc/hosts")),
    refused(call(C, run_subprocess, [])),
    stop(C).

child_info_reports_the_grants(Config) ->
    C = ctx(Config, #{dirs => [{data(Config), read}],
                      net => #{connect => [{tcp, <<"10.0.0.0/8">>, 443}]}}),
    {ok, Info} = py_context:child_info(C),
    Caps = maps:get(caps, Info),
    Dirs = maps:get(<<"dirs">>, Caps),
    Bin = list_to_binary(data(Config)),
    true = lists:keymember(Bin, 1, Dirs),
    #{<<"connect">> := [<<"tcp 10.0.0.0/8 443-443">>]} = maps:get(<<"net">>, Caps),
    stop(C).

caps_are_rejected_outside_isolated(_Config) ->
    {error, {caps_requires_isolated, worker}} =
        py_context:new(#{mode => worker, caps => #{}}),
    {error, {caps_requires_isolated, owngil}} =
        py_context:new(#{mode => owngil, caps => #{}}),
    ok.

%% A malformed rule is a configuration error and is reported as one here,
%% rather than met later as a refused connection: a capability set that
%% silently refuses everything looks exactly like one that works.
a_malformed_rule_is_a_configuration_error(_Config) ->
    {error, {bad_caps, {net, {address, <<"nope">>}}}} =
        py_context:new(#{mode => isolated,
                         caps => #{net => #{connect => [{tcp, <<"nope">>, 80}]}}}),
    {error, {bad_caps, {dir_not_absolute, "rel"}}} =
        py_context:new(#{mode => isolated, caps => #{dirs => [{"rel", read}]}}),
    {error, {bad_caps, {net, {port, -1}}}} =
        py_context:new(#{mode => isolated,
                         caps => #{net => #{listen => [{tcp, <<"127.0.0.1">>, -1}]}}}),
    {error, {bad_caps, {unknown_keys, [bogus]}}} =
        py_context:new(#{mode => isolated, caps => #{bogus => 1}}),
    ok.

no_caps_changes_nothing(Config) ->
    C = ctx(Config, no_caps),
    {ok, _} = read(C, "/etc/hosts"),
    {ok, 4} = py_context:eval(C, <<"2+2">>),
    stop(C).

%%% ============================================================================
%%% Helpers
%%% ============================================================================

ctx(Config, no_caps) ->
    new(Config, #{});
ctx(Config, Caps) ->
    new(Config, #{caps => Caps}).

new(Config, Extra) ->
    TestDir = ?config(test_dir, Config),
    Opts = maps:merge(#{mode => isolated, paths => [TestDir]}, Extra),
    {ok, C} = py_context:new(Opts),
    C.

stop(C) ->
    ok = py_context:stop(C).

%% Every case ends with the context still working: a refusal must not have
%% left the child broken.
alive(C) ->
    {ok, 4} = py_context:eval(C, <<"2+2">>).

data(Config) -> ?config(data, Config).

path(Config, Rel) -> filename:join(data(Config), Rel).

read(C, Path) ->
    call(C, read_file, [to_bin(Path)]).

call(C, Fun, Args) ->
    py_context:call(C, ?TEST_MOD, Fun, [to_bin(A) || A <- Args], #{}, 10000).

%% A capability error, and never a missing-file error: the refusal says
%% nothing about whether the path exists.
refused({error, {'CapabilityError', _}}) -> ok;
refused(Other) -> ct:fail({expected_refusal, Other}).

listener() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {ip, {127,0,0,1}},
                                     {active, false}, {backlog, 8}]),
    {ok, Port} = inet:port(LSock),
    {LSock, Port}.

free_port() ->
    {ok, S} = gen_tcp:listen(0, [{ip, {127,0,0,1}}]),
    {ok, P} = inet:port(S),
    ok = gen_tcp:close(S),
    P.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(I) when is_integer(I) -> I;
to_bin(T) when is_tuple(T) -> T.

flush() ->
    receive _ -> flush() after 0 -> ok end.
