#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/erlang_python/ebin

%%% @doc Benchmark script for erlang_python performance testing.
%%%
%%% Run with:
%%%   rebar3 compile && escript examples/benchmark.erl
%%%
%%% Options:
%%%   --quick       Run quick benchmark (fewer iterations)
%%%   --full        Run full benchmark (more iterations)
%%%   --concurrent  Focus on concurrency tests
%%%   --help        Show help

-mode(compile).

main(Args) ->
    %% Parse arguments
    Opts = parse_args(Args),

    case maps:get(help, Opts, false) of
        true ->
            print_help(),
            halt(0);
        false ->
            ok
    end,

    %% Start the application
    io:format("~n=== erlang_python Benchmark ===~n~n"),

    {ok, _} = application:ensure_all_started(erlang_python),

    %% Print system info
    print_system_info(),

    %% Run benchmarks based on options
    Mode = maps:get(mode, Opts, standard),
    run_benchmarks(Mode),

    io:format("~n=== Benchmark Complete ===~n"),
    halt(0).

parse_args(Args) ->
    parse_args(Args, #{mode => standard}).

parse_args([], Acc) -> Acc;
parse_args(["--quick" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => quick});
parse_args(["--full" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => full});
parse_args(["--concurrent" | Rest], Acc) ->
    parse_args(Rest, Acc#{mode => concurrent});
parse_args(["--help" | _], Acc) ->
    Acc#{help => true};
parse_args([_ | Rest], Acc) ->
    parse_args(Rest, Acc).

print_help() ->
    io:format("Usage: escript examples/benchmark.erl [OPTIONS]~n~n"),
    io:format("Options:~n"),
    io:format("  --quick       Run quick benchmark (fewer iterations)~n"),
    io:format("  --full        Run full benchmark (more iterations)~n"),
    io:format("  --concurrent  Focus on concurrency tests~n"),
    io:format("  --help        Show this help~n").

print_system_info() ->
    io:format("System Information:~n"),
    io:format("  Erlang/OTP: ~s~n", [erlang:system_info(otp_release)]),
    io:format("  Schedulers: ~p~n", [erlang:system_info(schedulers)]),
    io:format("  Python: "),
    {ok, PyVer} = py:version(),
    io:format("~s~n", [PyVer]),
    io:format("  Execution Mode: ~p~n", [py:execution_mode()]),
    io:format("  Num Contexts: ~p~n", [py_context_router:num_contexts()]),
    io:format("  Max Concurrent: ~p~n", [py_semaphore:max_concurrent()]),
    io:format("~n").

run_benchmarks(quick) ->
    io:format("Running quick benchmarks...~n~n"),
    bench_simple_call(100),
    bench_eval(100),
    bench_concurrent(10, 10),
    bench_streaming(100),
    bench_schedule_inline(20);

run_benchmarks(full) ->
    io:format("Running full benchmarks...~n~n"),
    bench_simple_call(10000),
    bench_eval(10000),
    bench_concurrent(100, 100),
    bench_concurrent(1000, 10),
    bench_streaming(1000),
    bench_type_conversion(1000),
    bench_semaphore(10000),
    bench_schedule_inline(500);

run_benchmarks(concurrent) ->
    io:format("Running concurrency benchmarks...~n~n"),
    bench_concurrent(10, 10),
    bench_concurrent(50, 20),
    bench_concurrent(100, 50),
    bench_concurrent(200, 100),
    bench_concurrent(500, 100),
    bench_concurrent(1000, 50);

run_benchmarks(standard) ->
    io:format("Running standard benchmarks...~n~n"),
    bench_simple_call(1000),
    bench_eval(1000),
    bench_concurrent(50, 50),
    bench_streaming(500),
    bench_type_conversion(500),
    bench_semaphore(5000),
    bench_schedule_inline(100).

%% Simple call benchmark
bench_simple_call(N) ->
    io:format("Benchmark: Simple py:call (math.sqrt)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:call(math, sqrt, [I])
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerCall = TimeMs / N,
    CallsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per call: ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p calls/sec~n~n", [round(CallsPerSec)]).

%% Eval benchmark
bench_eval(N) ->
    io:format("Benchmark: py:eval (arithmetic)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            {ok, _} = py:eval(<<"x * x">>, #{x => I})
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerCall = TimeMs / N,
    CallsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per eval: ~.3f ms~n", [PerCall]),
    io:format("  Throughput: ~p evals/sec~n~n", [round(CallsPerSec)]).

%% Concurrent call benchmark
bench_concurrent(NumProcs, CallsPerProc) ->
    TotalCalls = NumProcs * CallsPerProc,
    io:format("Benchmark: Concurrent calls~n"),
    io:format("  Processes: ~p, Calls/process: ~p, Total: ~p~n",
              [NumProcs, CallsPerProc, TotalCalls]),

    Parent = self(),

    {Time, _} = timer:tc(fun() ->
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(I) ->
                {ok, _} = py:call(math, sqrt, [I])
            end, lists:seq(1, CallsPerProc)),
            Parent ! {done, self()}
        end) || _ <- lists:seq(1, NumProcs)],

        [receive {done, Pid} -> ok end || Pid <- Pids]
    end),

    TimeMs = Time / 1000,
    CallsPerSec = TotalCalls / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Throughput: ~p calls/sec~n~n", [round(CallsPerSec)]).

%% Streaming benchmark
bench_streaming(N) ->
    io:format("Benchmark: Streaming (generator)~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            {ok, _} = py:stream_eval(<<"(x**2 for x in range(100))">>)
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerStream = TimeMs / N,
    StreamsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per stream: ~.3f ms~n", [PerStream]),
    io:format("  Throughput: ~p streams/sec~n~n", [round(StreamsPerSec)]).

%% Type conversion benchmark
bench_type_conversion(N) ->
    io:format("Benchmark: Type conversion (complex data)~n"),
    io:format("  Iterations: ~p~n", [N]),

    ComplexData = #{
        <<"name">> => <<"test">>,
        <<"numbers">> => lists:seq(1, 100),
        <<"nested">> => #{<<"a">> => 1, <<"b">> => 2}
    },

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            {ok, _} = py:call(json, dumps, [ComplexData])
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerConv = TimeMs / N,
    ConvsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per conversion: ~.3f ms~n", [PerConv]),
    io:format("  Throughput: ~p conversions/sec~n~n", [round(ConvsPerSec)]).

%% Semaphore benchmark
bench_semaphore(N) ->
    io:format("Benchmark: Semaphore acquire/release~n"),
    io:format("  Iterations: ~p~n", [N]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            ok = py_semaphore:acquire(1000),
            ok = py_semaphore:release()
        end, lists:seq(1, N))
    end),

    TimeMs = Time / 1000,
    PerOp = TimeMs / N,
    OpsPerSec = N / (TimeMs / 1000),

    io:format("  Total time: ~.2f ms~n", [TimeMs]),
    io:format("  Per acquire/release: ~.4f ms~n", [PerOp]),
    io:format("  Throughput: ~p ops/sec~n~n", [round(OpsPerSec)]).

%% Schedule inline vs schedule_py benchmark
bench_schedule_inline(N) ->
    io:format("Benchmark: schedule_inline vs schedule_py (chained calls)~n"),
    io:format("  Iterations: ~p~n", [N]),

    %% Set up test functions
    Ctx = py:context(),
    ok = py:exec(Ctx, <<"
import __main__
import erlang

def increment(x):
    return x + 1

__main__.increment = increment

# Chain using schedule_py (messaging overhead)
def chain_schedule_py(x, count):
    if count <= 0:
        return x
    x = erlang.schedule_py('__main__', 'increment', [x])
    return erlang.schedule_py('__main__', 'chain_schedule_py', [x, count - 1])

__main__.chain_schedule_py = chain_schedule_py

# Chain using schedule_inline (no messaging)
def chain_schedule_inline(x, count):
    if count <= 0:
        return x
    x = increment(x)
    return erlang.schedule_inline('__main__', 'chain_schedule_inline', [x, count - 1])

__main__.chain_schedule_inline = chain_schedule_inline
">>),

    ChainLen = 10,

    %% Benchmark schedule_py
    {TimePy, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            {ok, _} = py:call(Ctx, '__main__', chain_schedule_py, [0, ChainLen])
        end, lists:seq(1, N))
    end),

    %% Benchmark schedule_inline
    {TimeInline, _} = timer:tc(fun() ->
        lists:foreach(fun(_) ->
            {ok, _} = py:call(Ctx, '__main__', chain_schedule_inline, [0, ChainLen])
        end, lists:seq(1, N))
    end),

    TotalCalls = N * ChainLen,
    PyMs = TimePy / 1000,
    InlineMs = TimeInline / 1000,
    Speedup = PyMs / InlineMs,

    io:format("  Chain length: ~p~n", [ChainLen]),
    io:format("  Total chained calls: ~p~n", [TotalCalls]),
    io:format("~n"),
    io:format("  schedule_py:~n"),
    io:format("    Total: ~.2f ms~n", [PyMs]),
    io:format("    Per chain: ~.3f ms~n", [PyMs / N]),
    io:format("    Throughput: ~p chains/sec~n", [round(N / (PyMs / 1000))]),
    io:format("~n"),
    io:format("  schedule_inline:~n"),
    io:format("    Total: ~.2f ms~n", [InlineMs]),
    io:format("    Per chain: ~.3f ms~n", [InlineMs / N]),
    io:format("    Throughput: ~p chains/sec~n", [round(N / (InlineMs / 1000))]),
    io:format("~n"),
    io:format("  Speedup: ~.2fx~n~n", [Speedup]).
