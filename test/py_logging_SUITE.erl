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

%%% @doc Test suite for Python logging and tracing integration.
-module(py_logging_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_basic_logging/1,
    test_log_levels/1,
    test_log_from_thread/1,
    test_trace_basic/1,
    test_trace_nested/1,
    test_trace_decorator/1,
    test_trace_events/1,
    test_trace_error/1,
    test_configure_logging/1
]).

all() ->
    [
        test_basic_logging,
        test_log_levels,
        test_log_from_thread,
        test_trace_basic,
        test_trace_nested,
        test_trace_decorator,
        test_trace_events,
        test_trace_error,
        test_configure_logging
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(erlang_python),
    {ok, _} = py:start_contexts(),
    Config.

end_per_suite(_Config) ->
    application:stop(erlang_python),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Clear traces before each test
    py:clear_traces(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Disable tracing after each test
    py:disable_tracing(),
    ok.

%% @doc Test basic Python logging integration.
test_basic_logging(_Config) ->
    %% Configure logging
    ok = py:configure_logging(),

    %% Log a message from Python (use __import__ for single expression)
    {ok, _} = py:eval(<<"__import__('logging').info('test message from Python')">>),

    %% Give a moment for async delivery
    timer:sleep(50),

    %% Success if no error
    ok.

%% @doc Test log level filtering.
test_log_levels(_Config) ->
    %% Configure logging with warning level
    ok = py:configure_logging(#{level => warning}),

    %% Log at different levels using exec for multi-statement code
    ok = py:exec(<<"
import logging
logging.debug('debug msg')
logging.info('info msg')
logging.warning('warning msg')
logging.error('error msg')
">>),

    %% Give a moment for async delivery
    timer:sleep(50),

    ok.

%% @doc Test logging from Python threads.
test_log_from_thread(_Config) ->
    ok = py:configure_logging(),

    ok = py:exec(<<"
import logging
import threading

def log_in_thread():
    logging.info('message from thread')

t = threading.Thread(target=log_in_thread)
t.start()
t.join()
">>),

    timer:sleep(100),
    ok.

%% @doc Test basic trace span functionality.
test_trace_basic(_Config) ->
    ok = py:enable_tracing(),

    ok = py:exec(<<"
import erlang
with erlang.Span('test-span', key='value'):
    pass
">>),

    {ok, Spans} = py:get_traces(),
    1 = length(Spans),
    [Span] = Spans,
    <<"test-span">> = maps:get(name, Span),
    "ok" = maps:get(status, Span),  %% Status is now string since v2.0
    true = maps:get(duration_us, Span) >= 0,
    #{<<"key">> := <<"value">>} = maps:get(attributes, Span),
    ok.

%% @doc Test nested trace spans.
test_trace_nested(_Config) ->
    ok = py:enable_tracing(),

    ok = py:exec(<<"
import erlang
with erlang.Span('parent'):
    with erlang.Span('child'):
        pass
">>),

    {ok, Spans} = py:get_traces(),
    2 = length(Spans),

    %% Spans are in completion order (child completes first)
    [Child, Parent] = Spans,
    <<"child">> = maps:get(name, Child),
    <<"parent">> = maps:get(name, Parent),

    %% Child should have parent's span_id as parent_id
    ParentSpanId = maps:get(span_id, Parent),
    ParentSpanId = maps:get(parent_id, Child),

    ok.

%% @doc Test trace decorator.
test_trace_decorator(_Config) ->
    ok = py:enable_tracing(),

    ok = py:exec(<<"
import erlang

@erlang.trace()
def my_traced_function():
    return 42

result = my_traced_function()
">>),

    {ok, Spans} = py:get_traces(),
    1 = length(Spans),
    [Span] = Spans,
    %% Function name includes module
    Name = maps:get(name, Span),
    true = is_binary(Name),
    ok.

%% @doc Test span events.
test_trace_events(_Config) ->
    ok = py:enable_tracing(),

    ok = py:exec(<<"
import erlang
with erlang.Span('work') as span:
    span.event('checkpoint', items=10)
    span.event('progress', percent=50)
">>),

    {ok, [Span]} = py:get_traces(),
    Events = maps:get(events, Span),
    2 = length(Events),

    %% Events are in reverse order (most recent first)
    [Event2, Event1] = Events,
    <<"checkpoint">> = maps:get(name, Event1),
    <<"progress">> = maps:get(name, Event2),

    ok.

%% @doc Test span error handling.
test_trace_error(_Config) ->
    ok = py:enable_tracing(),

    ok = py:exec(<<"
import erlang
try:
    with erlang.Span('failing'):
        raise ValueError('intentional error')
except ValueError:
    pass
">>),

    {ok, [Span]} = py:get_traces(),
    "error" = maps:get(status, Span),  %% Status is now string since v2.0
    EndAttrs = maps:get(end_attrs, Span),
    true = maps:is_key(<<"exception">>, EndAttrs),
    ok.

%% @doc Test configure_logging with format.
test_configure_logging(_Config) ->
    %% Test with custom format
    ok = py:configure_logging(#{
        level => debug,
        format => <<"%(name)s - %(levelname)s - %(message)s">>
    }),

    {ok, _} = py:eval(<<"__import__('logging').info('formatted message')">>),
    timer:sleep(50),
    ok.
