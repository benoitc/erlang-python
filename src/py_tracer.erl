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

%%% @doc Python distributed tracing integration.
%%%
%%% This gen_server collects trace spans from Python code and provides
%%% an API to retrieve them.
%%%
%%% == Architecture ==
%%%
%%% ```
%%% Python:
%%%   with erlang.Span('my-operation'):
%%%       do_work()
%%%        |
%%%        v
%%% NIF: enif_send(span_start/span_end)
%%%        |
%%%        v
%%% py_tracer (this gen_server)
%%%   - Tracks active spans
%%%   - Collects completed spans
%%% '''
%%%
%%% == Usage ==
%%%
%%% ```erlang
%%% %% Enable tracing
%%% ok = py:enable_tracing().
%%%
%%% %% Run some Python code with spans
%%% {ok, _} = py:eval(<<"
%%% import erlang
%%% with erlang.Span('my-operation', key='value'):
%%%     pass
%%% ">>).
%%%
%%% %% Get collected spans
%%% {ok, Spans} = py:get_traces().
%%% %% Spans = [#{name => <<"my-operation">>, status => ok, ...}]
%%%
%%% %% Clear and disable
%%% ok = py:clear_traces().
%%% ok = py:disable_tracing().
%%% '''
%%%
%%% @private
-module(py_tracer).
-behaviour(gen_server).

-export([start_link/0, enable/0, disable/0, get_spans/0, clear/0]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2]).

%% @doc Start the tracer.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Enable tracing - start receiving spans from Python.
-spec enable() -> ok.
enable() ->
    gen_server:call(?MODULE, enable).

%% @doc Disable tracing - stop receiving spans.
-spec disable() -> ok.
disable() ->
    gen_server:call(?MODULE, disable).

%% @doc Get all completed spans.
-spec get_spans() -> {ok, [map()]}.
get_spans() ->
    gen_server:call(?MODULE, get_spans).

%% @doc Clear all collected spans.
-spec clear() -> ok.
clear() ->
    gen_server:call(?MODULE, clear).

%% @private
init([]) ->
    {ok, #{enabled => false, spans => #{}, completed => []}}.

%% @private
handle_call(enable, _From, State) ->
    py_nif:set_trace_receiver(self()),
    {reply, ok, State#{enabled => true}};

handle_call(disable, _From, State) ->
    py_nif:clear_trace_receiver(),
    {reply, ok, State#{enabled => false}};

handle_call(get_spans, _From, #{completed := Completed} = State) ->
    {reply, {ok, lists:reverse(Completed)}, State};

handle_call(clear, _From, State) ->
    {reply, ok, State#{spans => #{}, completed => []}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private Handle span events from Python
handle_info({span_start, SpanId, ParentId, Name, Attrs, StartTime},
            #{spans := Spans} = State) ->
    Span = #{
        span_id => SpanId,
        parent_id => ParentId,
        name => Name,
        attributes => Attrs,
        start_time => StartTime,
        events => []
    },
    {noreply, State#{spans => Spans#{SpanId => Span}}};

handle_info({span_end, SpanId, Status, Attrs, EndTime},
            #{spans := Spans, completed := Completed} = State) ->
    case maps:take(SpanId, Spans) of
        {Span, NewSpans} ->
            Done = Span#{
                status => Status,
                end_attrs => Attrs,
                end_time => EndTime,
                duration_us => EndTime - maps:get(start_time, Span)
            },
            {noreply, State#{spans => NewSpans, completed => [Done | Completed]}};
        error ->
            {noreply, State}
    end;

handle_info({span_event, SpanId, Name, Attrs, Time}, #{spans := Spans} = State) ->
    case maps:get(SpanId, Spans, undefined) of
        undefined ->
            {noreply, State};
        Span ->
            Events = maps:get(events, Span, []),
            Event = #{name => Name, attrs => Attrs, time => Time},
            NewSpan = Span#{events => [Event | Events]},
            {noreply, State#{spans => Spans#{SpanId => NewSpan}}}
    end;

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #{enabled := true}) ->
    py_nif:clear_trace_receiver(),
    ok;
terminate(_Reason, _State) ->
    ok.
