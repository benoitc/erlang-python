%%% @doc Common Test suite for shared py_buffers (`py_buffer:new(#{shared => true})'):
%%% the streaming input buffer over shared memory, in worker and isolated
%%% contexts. Mirrors py_buffer_SUITE where the case applies.
-module(py_isolated_buffer_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    test_read_all/1,
    test_read_n/1,
    test_readline/1,
    test_readlines_and_iter/1,
    test_read_blocks_until_write/1,
    test_read_nonblock_and_eof/1,
    test_backpressure/1,
    test_write_timeout/1,
    test_large_body_throughput/1,
    test_close_while_reading/1,
    test_wsgi_input_in_environ/1,
    test_read_with_nested_callback/1,
    test_write_after_close/1,
    test_restart_mid_body/1,
    test_native_buffer_refused_in_isolated/1
]).

-define(MOD, py_test_isolated_shm).
-define(MB, (1024 * 1024)).

all() ->
    [{group, worker}, {group, isolated}, {group, isolated_only}].

groups() ->
    Both = [
        test_read_all,
        test_read_n,
        test_readline,
        test_readlines_and_iter,
        test_read_blocks_until_write,
        test_read_nonblock_and_eof,
        test_backpressure,
        test_write_timeout,
        test_large_body_throughput,
        test_close_while_reading,
        test_wsgi_input_in_environ,
        test_read_with_nested_callback,
        test_write_after_close
    ],
    [{worker, [], Both},
     {isolated, [], Both},
     {isolated_only, [], [test_restart_mid_body, test_native_buffer_refused_in_isolated]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    case py_shm:available() of
        true -> [{test_dir, filename:join(code:lib_dir(erlang_python), "test")} | Config];
        false -> {skip, "iommap not available"}
    end.

end_per_suite(_Config) ->
    ok = application:stop(erlang_python),
    ok.

init_per_group(isolated_only, Config) -> [{mode, isolated} | Config];
init_per_group(Mode, Config) -> [{mode, Mode} | Config].

end_per_group(_Group, _Config) ->
    ok.

%%% ============================================================================

test_read_all(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    {ok, <<"SharedBuffer">>} = py_context:call(C, ?MOD, buf_kind, [Buf]),
    ok = py_buffer:write(Buf, <<"hello ">>),
    ok = py_buffer:write(Buf, <<"world">>),
    ok = py_buffer:close(Buf),
    {ok, <<"hello world">>} = py_context:call(C, ?MOD, buf_read_all, [Buf]),
    %% EOF: further reads return empty
    {ok, <<>>} = py_context:call(C, ?MOD, buf_read_all, [Buf]),
    stop(C).

test_read_n(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(Buf, <<"abcdefghij">>),
    ok = py_buffer:close(Buf),
    {ok, <<"abc">>} = py_context:call(C, ?MOD, buf_read_n, [Buf, 3]),
    {ok, [<<"defg">>, <<"hij">>]} = py_context:call(C, ?MOD, buf_read_chunks, [Buf, 4]),
    stop(C).

test_readline(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(Buf, <<"line one\nline ">>),
    ok = py_buffer:write(Buf, <<"two\nno newline">>),
    ok = py_buffer:close(Buf),
    {ok, <<"line one\n">>} = py_context:call(C, ?MOD, buf_readline, [Buf]),
    {ok, <<"line two\n">>} = py_context:call(C, ?MOD, buf_readline, [Buf]),
    {ok, <<"no newline">>} = py_context:call(C, ?MOD, buf_readline, [Buf]),
    {ok, <<>>} = py_context:call(C, ?MOD, buf_readline, [Buf]),
    stop(C).

test_readlines_and_iter(Config) ->
    C = new_ctx(Config),
    {ok, B1} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(B1, <<"a\nb\nc">>),
    ok = py_buffer:close(B1),
    {ok, [<<"a\n">>, <<"b\n">>, <<"c">>]} = py_context:call(C, ?MOD, buf_readlines, [B1]),
    {ok, B2} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(B2, <<"x\ny\n">>),
    ok = py_buffer:close(B2),
    {ok, [<<"x\n">>, <<"y\n">>]} = py_context:call(C, ?MOD, buf_iter, [B2]),
    stop(C).

%% @doc A read issued before any data blocks, then returns once written.
test_read_blocks_until_write(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    Self = self(),
    spawn_link(fun() -> Self ! {got, py_context:call(C, ?MOD, buf_read_n, [Buf, 5], #{}, 10000)} end),
    receive {got, _} -> ct:fail(read_returned_without_data) after 300 -> ok end,
    ok = py_buffer:write(Buf, <<"data!">>),
    receive {got, {ok, <<"data!">>}} -> ok after 5000 -> ct:fail(read_did_not_wake) end,
    ok = py_buffer:close(Buf),
    stop(C).

test_read_nonblock_and_eof(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    {ok, <<>>} = py_context:call(C, ?MOD, buf_read_nonblock, [Buf]),
    {ok, false} = py_context:call(C, ?MOD, buf_at_eof, [Buf]),
    ok = py_buffer:write(Buf, <<"ready">>),
    {ok, <<"ready">>} = py_context:call(C, ?MOD, buf_read_nonblock, [Buf]),
    ok = py_buffer:close(Buf),
    {ok, true} = py_context:call(C, ?MOD, buf_at_eof, [Buf]),
    stop(C).

%% @doc Body larger than the ring: the writer blocks while the reader
%% catches up and everything arrives in order.
test_backpressure(Config) ->
    C = new_ctx(Config),
    Ring = 64 * 1024,
    {ok, Buf} = py_buffer:new(#{shared => true, size => Ring}),
    Total = 10 * Ring + 123,
    Self = self(),
    spawn_link(fun() ->
        Self ! {read, py_context:call(C, ?MOD, buf_consume_checksum, [Buf, 7000], #{}, 60000)}
    end),
    Chunks = [crypto:strong_rand_bytes(11111) || _ <- lists:seq(1, Total div 11111)],
    Last = crypto:strong_rand_bytes(Total rem 11111),
    All = iolist_to_binary(Chunks ++ [Last]),
    T0 = erlang:monotonic_time(millisecond),
    [ok = py_buffer:write(Buf, Ch) || Ch <- Chunks ++ [Last]],
    ok = py_buffer:close(Buf),
    ct:log("wrote ~p bytes through a ~p ring in ~p ms",
           [Total, Ring, erlang:monotonic_time(millisecond) - T0]),
    Expected = {Total, checksum(All)},
    receive {read, {ok, {Got, Sum}}} -> Expected = {Got, Sum}
    after 60000 -> ct:fail(reader_hung)
    end,
    stop(C).

test_write_timeout(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true, size => 4096}),
    ok = py_buffer:write(Buf, binary:copy(<<1>>, 4096)),
    %% Nobody reads: the next write cannot fit and times out
    {error, timeout} = py_buffer:write(Buf, <<"more">>, 300),
    %% Reading frees the ring and later writes succeed
    Self = self(),
    spawn_link(fun() -> Self ! {n, py_context:call(C, ?MOD, buf_consume_len, [Buf, 4096], #{}, 10000)} end),
    ok = py_buffer:write(Buf, <<"more">>, 5000),
    ok = py_buffer:close(Buf),
    receive {n, {ok, 4100}} -> ok after 10000 -> ct:fail(reader_hung) end,
    stop(C).

test_large_body_throughput(Config) ->
    C = new_ctx(Config),
    Size = 64 * ?MB,
    Body = crypto:strong_rand_bytes(Size),
    {ok, Buf} = py_buffer:new(#{shared => true, size => 8 * ?MB}),
    Self = self(),
    spawn_link(fun() ->
        Self ! {read, py_context:call(C, ?MOD, buf_consume_len, [Buf, ?MB], #{}, 120000)}
    end),
    T0 = erlang:monotonic_time(microsecond),
    [ok = py_buffer:write(Buf, Chunk) || <<Chunk:?MB/binary>> <= Body],
    ok = py_buffer:close(Buf),
    receive {read, {ok, Size}} -> ok after 120000 -> ct:fail(reader_hung) end,
    Us = erlang:monotonic_time(microsecond) - T0,
    ct:log("64 MB through a shared buffer (~p): ~.1f ms, ~.1f MB/s",
           [?config(mode, Config), Us / 1000, Size / ?MB / (Us / 1.0e6)]),
    ct:print("shared buffer 64 MB (~p): ~.1f ms", [?config(mode, Config), Us / 1000]),
    stop(C).

test_close_while_reading(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    Self = self(),
    spawn_link(fun() -> Self ! {got, py_context:call(C, ?MOD, buf_read_all, [Buf], #{}, 10000)} end),
    timer:sleep(200),
    ok = py_buffer:close(Buf),
    receive {got, {ok, <<>>}} -> ok after 5000 -> ct:fail(read_did_not_return_on_close) end,
    stop(C).

test_wsgi_input_in_environ(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(Buf, <<"{\"json\": true}">>),
    ok = py_buffer:close(Buf),
    Environ = #{<<"method">> => <<"POST">>, <<"wsgi.input">> => Buf},
    {ok, {<<"POST">>, 14, <<"{\"json\":">>}} = py_context:call(C, ?MOD, buf_from_environ, [Environ]),
    stop(C).

%% @doc A callback re-entering the context while a read is in progress.
test_read_with_nested_callback(Config) ->
    C = new_ctx(Config),
    py_callback:register(<<"shm_double">>, fun([X]) -> X * 2 end),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(Buf, <<"headrest of the body">>),
    ok = py_buffer:close(Buf),
    {ok, {<<"head">>, 42, 16}} = py_context:call(C, ?MOD, buf_read_with_callback, [Buf, <<"shm_double">>]),
    py_callback:unregister(<<"shm_double">>),
    stop(C).

test_write_after_close(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:close(Buf),
    {error, closed} = py_buffer:write(Buf, <<"late">>),
    {ok, <<>>} = py_context:call(C, ?MOD, buf_read_all, [Buf]),
    stop(C).

%% @doc The child dies mid-body; the new child continues from the read
%% position (unread data is still in the ring).
test_restart_mid_body(Config) ->
    C = new_ctx(Config),
    {ok, Buf} = py_buffer:new(#{shared => true}),
    ok = py_buffer:write(Buf, <<"part one|">>),
    {ok, <<"part one|">>} = py_context:call(C, ?MOD, buf_read_n, [Buf, 9]),
    ok = py_buffer:write(Buf, <<"part two">>),
    ok = py_buffer:close(Buf),
    ok = py_context:kill(C),
    {ok, <<"part two">>} = py_context:call(C, ?MOD, buf_read_all, [Buf]),
    stop(C).

test_native_buffer_refused_in_isolated(Config) ->
    C = new_ctx(Config),
    {ok, Native} = py_buffer:new(),
    ok = py_buffer:write(Native, <<"x">>),
    ok = py_buffer:close(Native),
    %% A NIF resource cannot cross: it does not arrive as a buffer
    {ok, Kind} = py_context:call(C, ?MOD, buf_kind, [Native]),
    true = Kind =/= <<"SharedBuffer">> andalso Kind =/= <<"PyBuffer">>,
    stop(C).

%%% ============================================================================

checksum(Bin) ->
    lists:foldl(fun(B, Acc) -> (Acc + B) rem 1000003 end, 0, binary_to_list(Bin)).

new_ctx(Config) ->
    Mode = ?config(mode, Config),
    TestDir = ?config(test_dir, Config),
    {ok, C} = py_context:new(#{mode => Mode, paths => [TestDir]}),
    case Mode of
        worker ->
            ok = py_context:exec(C, iolist_to_binary(io_lib:format(
                "import sys\nif '~s' not in sys.path: sys.path.insert(0, '~s')", [TestDir, TestDir])));
        _ -> ok
    end,
    C.

stop(C) ->
    ok = py_context:stop(C),
    ok.
