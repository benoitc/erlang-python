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

%%% @doc Python logging integration with Erlang logger.
%%%
%%% This gen_server receives log messages from Python's logging module
%%% and forwards them to Erlang's logger.
%%%
%%% == Architecture ==
%%%
%%% ```
%%% Python logging.info("msg")
%%%        |
%%%        v
%%% ErlangHandler.emit()
%%%        |
%%%        v
%%% erlang._log(level, name, msg, meta)
%%%        |
%%%        NIF: enif_send()
%%%        |
%%%        v
%%% py_logger (this gen_server)
%%%        |
%%%        v
%%% logger:log(Level, Msg, Meta)
%%% '''
%%%
%%% == Usage ==
%%%
%%% The py_logger is automatically started by the erlang_python application.
%%% To configure Python logging from Erlang:
%%%
%%% ```
%%% ok = py:configure_logging().
%%% %% or with options:
%%% ok = py:configure_logging(#{level => info}).
%%% '''
%%%
%%% Then from Python:
%%% ```python
%%% import logging
%%% logging.info("This will appear in Erlang logs")
%%% '''
%%%
%%% @private
-module(py_logger).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2]).

%% @doc Start the logger with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the logger with options.
%% Options:
%%   level => debug | info | warning | error | critical
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @private
init(Opts) ->
    Level = maps:get(level, Opts, debug),
    LevelInt = level_to_int(Level),
    ok = py_nif:set_log_receiver(self(), LevelInt),
    {ok, #{level => Level}}.

%% @private Handle log messages from Python
handle_info({py_log, Level, Logger, Message, Meta, _Ts}, State) ->
    %% Forward to Erlang logger with Python metadata
    logger:log(Level, Message, #{
        domain => [python],
        py_logger => Logger,
        py_meta => Meta
    }),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    py_nif:clear_log_receiver(),
    ok.

%% @private Convert Erlang log level to Python levelno
level_to_int(debug) -> 0;
level_to_int(info) -> 10;
level_to_int(warning) -> 20;
level_to_int(error) -> 30;
level_to_int(critical) -> 40;
level_to_int(_) -> 0.
