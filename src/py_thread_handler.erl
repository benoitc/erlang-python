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

%%% @doc Thread worker handler for Python ThreadPoolExecutor support.
%%%
%%% This module enables Python threads spawned via concurrent.futures.ThreadPoolExecutor
%%% to call erlang.call() without blocking.
%%%
%%% == Architecture ==
%%%
%%% One Erlang process per Python thread - lightweight and isolated.
%%%
%%% The coordinator process manages the mapping of worker IDs to handler
%%% processes. When a Python thread first calls erlang.call(), the coordinator
%%% spawns a dedicated handler process for that thread.
%%%
%%% == Message Flow ==
%%%
%%% 1. Python thread calls erlang.call()
%%% 2. C code sends {thread_worker_spawn, WorkerId, WriteFd} to coordinator
%%% 3. Coordinator spawns handler, signals readiness via pipe
%%% 4. C code sends {thread_callback, WorkerId, CallbackId, FuncName, Args}
%%% 5. Handler executes callback and writes response via pipe
%%% 6. Python thread receives response and continues
%%%
%%% @private
-module(py_thread_handler).

-behaviour(gen_server).

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% State: map of WorkerId => {HandlerPid, WriteFd}
-record(state, {
    handlers = #{} :: #{non_neg_integer() => {pid(), integer()}}
}).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([]) ->
    %% Register ourselves as the thread worker coordinator
    case py_nif:thread_worker_set_coordinator(self()) of
        ok ->
            {ok, #state{}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle spawn request from Python thread
handle_info({thread_worker_spawn, WorkerId, WriteFd}, #state{handlers = Handlers} = State) ->
    %% Spawn a dedicated handler process for this thread
    HandlerPid = spawn_link(fun() -> handler_loop(WorkerId, WriteFd) end),

    %% Signal readiness to Python (write 0 length to indicate success)
    py_nif:thread_worker_signal_ready(WriteFd),

    %% Store handler mapping
    NewHandlers = Handlers#{WorkerId => {HandlerPid, WriteFd}},
    {noreply, State#state{handlers = NewHandlers}};

%% Handle callback request from Python thread
handle_info({thread_callback, WorkerId, CallbackId, FuncName, Args},
            #state{handlers = Handlers} = State) ->
    case maps:get(WorkerId, Handlers, undefined) of
        {HandlerPid, _WriteFd} ->
            %% Forward to the handler process
            HandlerPid ! {thread_callback, CallbackId, FuncName, Args};
        undefined ->
            %% Worker not found - this shouldn't happen if spawn was called first
            ok
    end,
    {noreply, State};

%% Handle async callback request from Python async_call()
%% Unlike thread_callback, this uses a global pipe for all async callbacks.
%% Each response includes the callback_id so Python can match it to the right Future.
handle_info({async_callback, CallbackId, FuncName, Args, WriteFd}, State) ->
    %% Spawn a process to handle this callback asynchronously
    %% This allows multiple async callbacks to be processed concurrently
    spawn_link(fun() ->
        handle_async_callback(WriteFd, CallbackId, FuncName, Args)
    end),
    {noreply, State};

%% Handle handler process exit
handle_info({'EXIT', Pid, _Reason}, #state{handlers = Handlers} = State) ->
    %% Remove handler from map
    NewHandlers = maps:filter(
        fun(_WorkerId, {HandlerPid, _Fd}) -> HandlerPid =/= Pid end,
        Handlers),
    {noreply, State#state{handlers = NewHandlers}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%% ============================================================================
%%% Handler Process
%%% ============================================================================

%% Simple receive loop for handling callbacks from a specific Python thread.
%% Each handler is dedicated to one Python thread and has its own response pipe.
handler_loop(WorkerId, WriteFd) ->
    receive
        {thread_callback, _CallbackId, FuncName, Args} ->
            %% Execute the callback and send response
            handle_thread_callback(WriteFd, FuncName, Args),
            handler_loop(WorkerId, WriteFd);

        {pipe_closed} ->
            %% Python thread exited, clean up
            ok;

        shutdown ->
            ok;

        _Other ->
            handler_loop(WorkerId, WriteFd)
    end.

%% Execute a callback and write response to the pipe
handle_thread_callback(WriteFd, FuncName, Args) ->
    %% Convert Args from tuple to list if needed
    ArgsList = case Args of
        T when is_tuple(T) -> tuple_to_list(T);
        L when is_list(L) -> L;
        _ -> [Args]
    end,

    %% Execute the registered function
    Response = case py_callback:execute(FuncName, ArgsList) of
        {ok, Result} ->
            %% Encode result as Python-parseable string
            %% Format: status_byte (0=ok) + python_repr
            ResultStr = term_to_python_repr(Result),
            <<0, ResultStr/binary>>;
        {error, {not_found, Name}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("Function '~s' not registered", [Name])),
            <<1, ErrMsg/binary>>;
        {error, {Class, Reason, _Stack}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("~p: ~p", [Class, Reason])),
            <<1, ErrMsg/binary>>
    end,

    %% Write response to pipe
    py_nif:thread_worker_write(WriteFd, Response).

%% Execute an async callback and write response to the async callback pipe.
%% Unlike handle_thread_callback, this includes the callback_id in the response
%% so Python can match it to the correct Future.
handle_async_callback(WriteFd, CallbackId, FuncName, Args) ->
    %% Convert Args from tuple to list if needed
    ArgsList = case Args of
        T when is_tuple(T) -> tuple_to_list(T);
        L when is_list(L) -> L;
        _ -> [Args]
    end,

    %% Execute the registered function
    Response = case py_callback:execute(FuncName, ArgsList) of
        {ok, Result} ->
            %% Encode result as Python-parseable string
            %% Format: status_byte (0=ok) + python_repr
            ResultStr = term_to_python_repr(Result),
            <<0, ResultStr/binary>>;
        {error, {not_found, Name}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("Function '~s' not registered", [Name])),
            <<1, ErrMsg/binary>>;
        {error, {Class, Reason, _Stack}} ->
            ErrMsg = iolist_to_binary(
                io_lib:format("~p: ~p", [Class, Reason])),
            <<1, ErrMsg/binary>>
    end,

    %% Write response to async callback pipe (includes callback_id)
    py_nif:async_callback_response(WriteFd, CallbackId, Response).

%%% ============================================================================
%%% Term to Python repr conversion
%%% (Same as py_worker.erl - could be factored out to py_util.erl)
%%% ============================================================================

%% Convert Erlang term to Python-parseable string representation
term_to_python_repr(Term) when is_integer(Term) ->
    integer_to_binary(Term);
term_to_python_repr(Term) when is_float(Term) ->
    float_to_binary(Term, [{decimals, 17}, compact]);
term_to_python_repr(true) ->
    <<"True">>;
term_to_python_repr(false) ->
    <<"False">>;
term_to_python_repr(none) ->
    <<"None">>;
term_to_python_repr(nil) ->
    <<"None">>;
term_to_python_repr(undefined) ->
    <<"None">>;
term_to_python_repr(Term) when is_atom(Term) ->
    %% Convert atom to Python string
    AtomStr = atom_to_binary(Term, utf8),
    <<"\"", AtomStr/binary, "\"">>;
term_to_python_repr(Term) when is_binary(Term) ->
    %% Escape binary as Python string
    Escaped = escape_string(Term),
    <<"\"", Escaped/binary, "\"">>;
term_to_python_repr(Term) when is_list(Term) ->
    %% Check if it's a string (list of integers)
    case io_lib:printable_list(Term) of
        true ->
            Bin = list_to_binary(Term),
            Escaped = escape_string(Bin),
            <<"\"", Escaped/binary, "\"">>;
        false ->
            Items = [term_to_python_repr(E) || E <- Term],
            Joined = join_binaries(Items, <<", ">>),
            <<"[", Joined/binary, "]">>
    end;
term_to_python_repr(Term) when is_tuple(Term) ->
    Items = [term_to_python_repr(E) || E <- tuple_to_list(Term)],
    Joined = join_binaries(Items, <<", ">>),
    case length(Items) of
        1 -> <<"(", Joined/binary, ",)">>;
        _ -> <<"(", Joined/binary, ")">>
    end;
term_to_python_repr(Term) when is_map(Term) ->
    Items = maps:fold(fun(K, V, Acc) ->
        KeyRepr = term_to_python_repr(K),
        ValRepr = term_to_python_repr(V),
        [<<KeyRepr/binary, ": ", ValRepr/binary>> | Acc]
    end, [], Term),
    Joined = join_binaries(Items, <<", ">>),
    <<"{", Joined/binary, "}">>;
term_to_python_repr(Term) when is_pid(Term) ->
    %% Encode PID using ETF (Erlang Term Format) for exact reconstruction.
    %% Format: "__etf__:<base64_encoded_binary>"
    %% The C side will detect this, base64 decode, and use enif_binary_to_term
    %% to reconstruct the pid, then convert to ErlangPidObject.
    Etf = term_to_binary(Term),
    B64 = base64:encode(Etf),
    <<"\"__etf__:", B64/binary, "\"">>;
term_to_python_repr(Term) when is_reference(Term) ->
    %% References also need ETF encoding for round-trip
    Etf = term_to_binary(Term),
    B64 = base64:encode(Etf),
    <<"\"__etf__:", B64/binary, "\"">>;
term_to_python_repr(_Term) ->
    %% Fallback - return None for unsupported types
    <<"None">>.

escape_string(Bin) ->
    %% Escape special characters for Python string
    binary:replace(
        binary:replace(
            binary:replace(
                binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
                <<"\"">>, <<"\\\"">>, [global]),
            <<"\n">>, <<"\\n">>, [global]),
        <<"\r">>, <<"\\r">>, [global]).

join_binaries([], _Sep) -> <<>>;
join_binaries([H], _Sep) -> H;
join_binaries([H|T], Sep) ->
    lists:foldl(fun(E, Acc) -> <<Acc/binary, Sep/binary, E/binary>> end, H, T).
