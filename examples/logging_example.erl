#!/usr/bin/env escript
%%% @doc Logging and tracing example - demonstrates Python/Erlang integration.
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/logging_example.erl

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Python Logging and Tracing Demo ===~n~n"),

    %% Part 1: Logging
    logging_demo(),

    %% Part 2: Tracing
    tracing_demo(),

    io:format("=== Done ===~n~n"),
    ok.

logging_demo() ->
    io:format("--- Logging Demo ---~n~n"),

    %% Configure Python logging to forward to Erlang
    io:format("Configuring Python logging...~n"),
    ok = py:configure_logging(#{level => debug}),

    %% Log messages from Python
    io:format("Sending log messages from Python:~n"),
    ok = py:exec(<<"
import logging

# Create a custom logger
logger = logging.getLogger('myapp')

logger.debug('Debug: detailed diagnostic info')
logger.info('Info: normal operation message')
logger.warning('Warning: something unexpected')
logger.error('Error: something failed')
">>),

    %% Give async messages time to be delivered
    timer:sleep(100),
    io:format("~n"),
    ok.

tracing_demo() ->
    io:format("--- Tracing Demo ---~n~n"),

    %% Enable tracing
    io:format("Enabling distributed tracing...~n"),
    ok = py:enable_tracing(),

    %% Create some traced operations
    io:format("Running traced Python code...~n~n"),
    ok = py:exec(<<"
import erlang
import time

# Basic span
with erlang.Span('process-order', order_id=12345):
    time.sleep(0.001)  # Simulate work

# Nested spans
with erlang.Span('handle-request', endpoint='/api/users'):
    with erlang.Span('validate-input'):
        time.sleep(0.0005)
    with erlang.Span('query-database', table='users'):
        time.sleep(0.001)
    with erlang.Span('format-response'):
        time.sleep(0.0002)

# Span with events
with erlang.Span('batch-process') as span:
    span.event('started', batch_size=100)
    time.sleep(0.0005)
    span.event('progress', completed=50, remaining=50)
    time.sleep(0.0005)
    span.event('finished', total_processed=100)

# Traced function using decorator
@erlang.trace()
def calculate_something():
    time.sleep(0.0002)
    return 42

result = calculate_something()

# Span with error
try:
    with erlang.Span('risky-operation'):
        raise ValueError('Something went wrong!')
except ValueError:
    pass
">>),

    %% Retrieve and display spans
    {ok, Spans} = py:get_traces(),
    io:format("Collected ~p spans:~n~n", [length(Spans)]),

    lists:foreach(fun(Span) ->
        Name = maps:get(name, Span),
        Status = maps:get(status, Span),
        Duration = maps:get(duration_us, Span),
        ParentId = maps:get(parent_id, Span),
        Events = maps:get(events, Span, []),
        Attrs = maps:get(attributes, Span),

        ParentStr = case ParentId of
            undefined -> "none";
            _ -> integer_to_list(ParentId rem 10000) ++ "..."
        end,

        io:format("  ~s~n", [Name]),
        io:format("    Status: ~p, Duration: ~p us~n", [Status, Duration]),
        io:format("    Parent: ~s~n", [ParentStr]),

        case maps:size(Attrs) of
            0 -> ok;
            _ -> io:format("    Attributes: ~p~n", [Attrs])
        end,

        case length(Events) of
            0 -> ok;
            N -> io:format("    Events: ~p~n", [N])
        end,

        io:format("~n")
    end, Spans),

    %% Clean up
    ok = py:clear_traces(),
    ok = py:disable_tracing(),
    ok.
