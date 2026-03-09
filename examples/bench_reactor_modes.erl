#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark comparing Reactor (worker vs subinterp) with Channel API.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/bench_reactor_modes.erl

-mode(compile).

main(_Args) ->
    io:format("~n"),
    io:format("========================================================~n"),
    io:format("  Reactor Modes vs Channel API Benchmark~n"),
    io:format("========================================================~n~n"),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),

    %% Print system info
    print_system_info(),

    %% Channel benchmarks
    io:format("~n--- Channel API ---~n"),
    {ChPersistent, ChLifecycle} = run_channel_bench(),

    %% Worker mode reactor benchmarks
    io:format("~n--- Reactor (Worker Mode) ---~n"),
    {WkPersistent, WkLifecycle} = run_reactor_worker_bench(),

    %% Subinterpreter mode benchmarks (if supported)
    {SiPersistent, SiLifecycle} = case py:subinterp_supported() of
        true ->
            io:format("~n--- Reactor (Subinterpreter Mode) ---~n"),
            run_reactor_subinterp_bench();
        false ->
            io:format("~n[Skipping subinterpreter benchmarks - Python < 3.12]~n"),
            {[], []}
    end,

    %% Print comparison summary
    print_comparison(ChPersistent, ChLifecycle, WkPersistent, WkLifecycle, SiPersistent, SiLifecycle),

    halt(0).

print_system_info() ->
    io:format("System Information~n"),
    io:format("------------------~n"),
    io:format("  Erlang/OTP:       ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers:       ~p~n", [erlang:system_info(schedulers)]),
    {ok, PyVer} = py:version(),
    io:format("  Python:           ~s~n", [PyVer]),
    io:format("  Subinterp:        ~p~n", [py:subinterp_supported()]),
    io:format("~n").

%% ============================================================================
%% Channel Benchmarks
%% ============================================================================

run_channel_bench() ->
    ok = py_channel:register_callbacks(),
    Sizes = [64, 1024, 16384],

    %% First: Messages on persistent channel (no creation overhead)
    io:format("  A) Messages on persistent channel (1000 msgs):~n"),
    PersistentResults = lists:map(fun(Size) ->
        {ok, Ch} = py_channel:new(),
        Data = binary:copy(<<$X>>, Size),
        Iterations = 1000,

        %% Warm up
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, 100)),

        %% Benchmark messages on persistent channel
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        py_channel:close(Ch),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    %% Second: Full lifecycle (create + send + receive + close)
    io:format("~n  B) Full lifecycle per op (create+send+recv+close, 200 ops):~n"),
    LifecycleResults = lists:map(fun(Size) ->
        Data = binary:copy(<<$X>>, Size),
        Iterations = 200,

        %% Benchmark full lifecycle
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            {ok, Ch} = py_channel:new(),
            ok = py_channel:send(Ch, Data),
            {ok, _} = py_nif:channel_try_receive(Ch),
            py_channel:close(Ch)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    %% Return persistent results for comparison (more representative)
    {PersistentResults, LifecycleResults}.

%% ============================================================================
%% Reactor Worker Mode Benchmarks
%% ============================================================================

run_reactor_worker_bench() ->
    %% Protocol that stays open for multiple messages
    PersistentSetup = <<"
import erlang.reactor as reactor

class PersistentEchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'read_pending'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'read_pending'

reactor.set_protocol_factory(PersistentEchoProtocol)
">>,

    %% Protocol that closes after one message
    LifecycleSetup = <<"
import erlang.reactor as reactor

class OneMessageProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(OneMessageProtocol)
">>,

    Sizes = [64, 1024, 16384],

    %% First: Messages on persistent connection
    io:format("  A) Messages on persistent connection (500 msgs):~n"),
    PersistentResults = lists:map(fun(Size) ->
        Data = binary:copy(<<$X>>, Size),
        Iterations = 500,

        {ok, Ctx} = py_reactor_context:start_link(900 + Size, worker, #{
            setup_code => PersistentSetup
        }),

        %% Create one persistent connection
        {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
        {ok, Port} = inet:port(LSock),
        {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        {ok, Server} = gen_tcp:accept(LSock, 1000),
        gen_tcp:close(LSock),
        {ok, Fd} = inet:getfd(Server),
        ok = py_reactor_context:handoff(Ctx, Fd, #{type => bench}),
        timer:sleep(10),

        %% Warm up
        lists:foreach(fun(_) ->
            ok = gen_tcp:send(Client, Data),
            {ok, _} = recv_all(Client, Size, 5000)
        end, lists:seq(1, 50)),

        %% Benchmark messages on persistent connection
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = gen_tcp:send(Client, Data),
            {ok, _} = recv_all(Client, Size, 5000)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        gen_tcp:close(Client),
        py_reactor_context:stop(Ctx),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    %% Second: Full lifecycle per operation
    io:format("~n  B) Full lifecycle per op (connect+handoff+echo+close, 100 ops):~n"),
    LifecycleResults = lists:map(fun(Size) ->
        Data = binary:copy(<<$X>>, Size),
        Iterations = 100,

        {ok, Ctx} = py_reactor_context:start_link(800 + Size, worker, #{
            setup_code => LifecycleSetup
        }),

        %% Warm up
        lists:foreach(fun(_) ->
            run_single_reactor_test(Ctx, Data)
        end, lists:seq(1, 10)),

        %% Benchmark full lifecycle
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            run_single_reactor_test(Ctx, Data)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        py_reactor_context:stop(Ctx),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    {PersistentResults, LifecycleResults}.

%% Helper: run single reactor round-trip test
run_single_reactor_test(Ctx, Data) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(LSock, 1000),
    gen_tcp:close(LSock),
    {ok, Fd} = inet:getfd(Server),

    %% Handoff FD to reactor (transfers ownership)
    ok = py_reactor_context:handoff(Ctx, Fd, #{type => bench}),

    %% Send data and receive full response
    ok = gen_tcp:send(Client, Data),
    {ok, _Response} = recv_all(Client, byte_size(Data), 5000),

    %% Clean up client side only (reactor owns the server FD)
    gen_tcp:close(Client),
    ok.

%% Helper: receive exactly N bytes from socket
recv_all(Socket, Size, Timeout) ->
    recv_all(Socket, Size, Timeout, <<>>).

recv_all(_Socket, 0, _Timeout, Acc) ->
    {ok, Acc};
recv_all(Socket, Remaining, Timeout, Acc) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            NewAcc = <<Acc/binary, Data/binary>>,
            Received = byte_size(Data),
            recv_all(Socket, Remaining - Received, Timeout, NewAcc);
        {error, Reason} ->
            {error, Reason}
    end.

%% ============================================================================
%% Reactor Subinterpreter Mode Benchmarks
%% ============================================================================

run_reactor_subinterp_bench() ->
    %% Protocol that stays open for multiple messages
    PersistentSetup = <<"
import erlang.reactor as reactor

class PersistentEchoProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'read_pending'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'read_pending'

reactor.set_protocol_factory(PersistentEchoProtocol)
">>,

    %% Protocol that closes after one message
    LifecycleSetup = <<"
import erlang.reactor as reactor

class OneMessageProtocol(reactor.Protocol):
    def data_received(self, data):
        self.write_buffer.extend(data)
        return 'write_pending'

    def write_ready(self):
        if not self.write_buffer:
            return 'close'
        written = self.write(bytes(self.write_buffer))
        del self.write_buffer[:written]
        return 'continue' if self.write_buffer else 'close'

reactor.set_protocol_factory(OneMessageProtocol)
">>,

    Sizes = [64, 1024, 16384],

    %% First: Messages on persistent connection
    io:format("  A) Messages on persistent connection (500 msgs):~n"),
    PersistentResults = lists:map(fun(Size) ->
        Data = binary:copy(<<$X>>, Size),
        Iterations = 500,

        {ok, Ctx} = py_reactor_context:start_link(1900 + Size, subinterp, #{
            setup_code => PersistentSetup
        }),

        %% Create one persistent connection
        {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
        {ok, Port} = inet:port(LSock),
        {ok, Client} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        {ok, Server} = gen_tcp:accept(LSock, 1000),
        gen_tcp:close(LSock),
        {ok, Fd} = inet:getfd(Server),
        ok = py_reactor_context:handoff(Ctx, Fd, #{type => bench}),
        timer:sleep(10),

        %% Warm up
        lists:foreach(fun(_) ->
            ok = gen_tcp:send(Client, Data),
            {ok, _} = recv_all(Client, Size, 5000)
        end, lists:seq(1, 50)),

        %% Benchmark messages on persistent connection
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            ok = gen_tcp:send(Client, Data),
            {ok, _} = recv_all(Client, Size, 5000)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        gen_tcp:close(Client),
        py_reactor_context:stop(Ctx),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    %% Second: Full lifecycle per operation
    io:format("~n  B) Full lifecycle per op (connect+handoff+echo+close, 100 ops):~n"),
    LifecycleResults = lists:map(fun(Size) ->
        Data = binary:copy(<<$X>>, Size),
        Iterations = 100,

        {ok, Ctx} = py_reactor_context:start_link(1800 + Size, subinterp, #{
            setup_code => LifecycleSetup
        }),

        %% Warm up
        lists:foreach(fun(_) ->
            run_single_reactor_test(Ctx, Data)
        end, lists:seq(1, 10)),

        %% Benchmark full lifecycle
        Start = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            run_single_reactor_test(Ctx, Data)
        end, lists:seq(1, Iterations)),
        End = erlang:monotonic_time(microsecond),

        py_reactor_context:stop(Ctx),

        TimeMs = (End - Start) / 1000,
        OpsPerSec = round(Iterations / (TimeMs / 1000)),
        LatencyUs = (End - Start) / Iterations,

        io:format("    Size ~6B: ~8.1f us/op, ~8w ops/sec~n",
                  [Size, LatencyUs, OpsPerSec]),

        {Size, LatencyUs, OpsPerSec}
    end, Sizes),

    {PersistentResults, LifecycleResults}.

%% ============================================================================
%% Comparison Summary
%% ============================================================================

print_comparison(ChPersistent, ChLifecycle, WkPersistent, WkLifecycle, SiPersistent, SiLifecycle) ->
    io:format("~n"),
    io:format("========================================================~n"),
    io:format("  COMPARISON SUMMARY~n"),
    io:format("========================================================~n~n"),

    %% Persistent connection comparison (messages/sec on existing connection)
    io:format("A) PERSISTENT CONNECTION (messages on existing connection)~n"),
    io:format("-----------------------------------------------------------~n"),
    io:format("~8s | ~12s | ~12s | ~12s~n",
              ["Size", "Channel", "Reactor/W", "Reactor/S"]),
    io:format("~s~n", [string:copies("-", 52)]),

    lists:foreach(fun({{Size, _, ChOps}, {_, _, WkOps}}) ->
        SubOps = case lists:keyfind(Size, 1, SiPersistent) of
            {_, _, O} -> integer_to_list(O);
            false -> "N/A"
        end,
        io:format("~8B | ~12w | ~12w | ~12s~n",
                  [Size, ChOps, WkOps, SubOps])
    end, lists:zip(ChPersistent, WkPersistent)),

    %% Full lifecycle comparison (connections/sec including setup/teardown)
    io:format("~n"),
    io:format("B) FULL LIFECYCLE (create + send/recv + close per op)~n"),
    io:format("-----------------------------------------------------------~n"),
    io:format("~8s | ~12s | ~12s | ~12s~n",
              ["Size", "Channel", "Reactor/W", "Reactor/S"]),
    io:format("~s~n", [string:copies("-", 52)]),

    lists:foreach(fun({{Size, _, ChOps}, {_, _, WkOps}}) ->
        SubOps = case lists:keyfind(Size, 1, SiLifecycle) of
            {_, _, O} -> integer_to_list(O);
            false -> "N/A"
        end,
        io:format("~8B | ~12w | ~12w | ~12s~n",
                  [Size, ChOps, WkOps, SubOps])
    end, lists:zip(ChLifecycle, WkLifecycle)),

    io:format("~n"),
    io:format("Legend:~n"),
    io:format("  Channel   = py_channel API~n"),
    io:format("  Reactor/W = erlang.reactor with worker mode~n"),
    io:format("  Reactor/S = erlang.reactor with subinterpreter (SHARED_GIL)~n"),
    io:format("~n"),
    io:format("Notes:~n"),
    io:format("  - A) measures throughput on persistent connection (best case)~n"),
    io:format("  - B) measures full lifecycle including setup/teardown~n"),
    io:format("  - Channel is queue-based, Reactor goes through TCP stack~n"),
    io:format("~n").
