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

%% State:
%%   handlers       — sync thread workers: WorkerId => {HandlerPid, WriteFd}
%%   async_writers  — async pipes: WriteFd => WriterPid (one writer process
%%                    per pipe; serialises py_nif:async_callback_response/3
%%                    so frames cannot interleave on the wire).
%%   writer_fds     — reverse map: WriterPid => WriteFd (fast lookup on
%%                    'DOWN' messages).
-record(state, {
    handlers      = #{} :: #{non_neg_integer() => {pid(), integer()}},
    async_writers = #{} :: #{integer() => pid()},
    writer_fds    = #{} :: #{pid() => integer()}
}).

%% Bound on async-writer mailbox length. Submissions beyond this point
%% are failed inline (the writer serialises an error frame for the
%% callback) so a stalled Python event loop cannot grow the writer's
%% mailbox without bound.
-define(ASYNC_WRITER_MAX_QUEUE, 10000).

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
    %% Trap exits so a dying handler / async writer is reaped via
    %% handle_info({'EXIT', ...}) instead of taking the coordinator
    %% down. (Also enables the 'DOWN' clause for monitored writers,
    %% which we route through the same cleanup logic.)
    process_flag(trap_exit, true),
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

%% Handle async callback request from Python async_call().
%%
%% Unlike thread_callback, this uses one shared per-interpreter pipe
%% for all async callbacks. To prevent frame-on-the-wire interleaving
%% from concurrent writers (issue #63), every response is funnelled
%% through a single writer process per WriteFd: callback execution
%% stays parallel, only the pipe write is serialised.
handle_info({async_callback, CallbackId, FuncName, Args, WriteFd}, State0) ->
    {Writer, State1} = ensure_async_writer(WriteFd, State0),
    case writer_overload(Writer) of
        true ->
            %% Backpressure: surface "queue full" as a normal error
            %% frame on the pipe so the Future resolves with a clear
            %% RuntimeError instead of growing the writer's mailbox
            %% indefinitely.
            Writer ! {overflow, CallbackId};
        false ->
            spawn(fun() ->
                Response = run_async_callback(FuncName, Args),
                Writer ! {respond, CallbackId, Response}
            end)
    end,
    {noreply, State1};

%% Handle linked sync handler exit.
handle_info({'EXIT', Pid, _Reason}, #state{handlers = Handlers} = State) ->
    NewHandlers = maps:filter(
        fun(_WorkerId, {HandlerPid, _Fd}) -> HandlerPid =/= Pid end,
        Handlers),
    {noreply, State#state{handlers = NewHandlers}};

%% Handle monitored async writer exit (writer hits a hard write error).
handle_info({'DOWN', _MRef, process, Pid, _Reason},
            #state{async_writers = Writers,
                   writer_fds    = WriterFds} = State) ->
    case maps:take(Pid, WriterFds) of
        {WriteFd, NewWriterFds} ->
            NewWriters = maps:remove(WriteFd, Writers),
            {noreply, State#state{async_writers = NewWriters,
                                  writer_fds    = NewWriterFds}};
        error ->
            {noreply, State}
    end;

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
        {thread_callback, CallbackId, FuncName, Args} ->
            %% Execute the callback and send response. If the write fails
            %% (pipe closed by the C side after worker poisoning, or
            %% write_timeout) we exit so the coordinator's trap_exit
            %% clause removes us from its handler map.
            handle_thread_callback(WriteFd, CallbackId, FuncName, Args),
            handler_loop(WorkerId, WriteFd);

        {pipe_closed} ->
            %% Python thread exited, clean up
            ok;

        shutdown ->
            ok;

        _Other ->
            handler_loop(WorkerId, WriteFd)
    end.

%% Execute a callback and write the id-prefixed response to the pipe.
handle_thread_callback(WriteFd, CallbackId, FuncName, Args) ->
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

    %% Write response to pipe; exit on failure so we are reaped.
    case py_nif:thread_worker_write_with_id(WriteFd, CallbackId, Response) of
        ok -> ok;
        {error, Reason1} ->
            exit({thread_worker_write_failed, Reason1})
    end.

%% Execute the user's registered function and encode the result as a
%% wire-format response body (`<<Status, PythonRepr/binary>>`).
%% Used by async_writer_loop (and indirectly by handle_thread_callback
%% via the same encoding shape — the sync path keeps its own copy
%% close to the write call site).
run_async_callback(FuncName, Args) ->
    ArgsList = case Args of
        T when is_tuple(T) -> tuple_to_list(T);
        L when is_list(L) -> L;
        _ -> [Args]
    end,
    case py_callback:execute(FuncName, ArgsList) of
        {ok, Result} ->
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
    end.

%% Look up or spawn the async writer for a given WriteFd. Writers are
%% monitored (not linked) so a writer death does not take the
%% coordinator down; the 'DOWN' clause cleans up the maps.
ensure_async_writer(WriteFd,
                    #state{async_writers = Writers,
                           writer_fds    = WriterFds} = State) ->
    case maps:get(WriteFd, Writers, undefined) of
        undefined ->
            Writer = spawn(fun() -> async_writer_loop(WriteFd) end),
            erlang:monitor(process, Writer),
            {Writer, State#state{
                async_writers = Writers#{WriteFd => Writer},
                writer_fds    = WriterFds#{Writer => WriteFd}}};
        Existing ->
            {Existing, State}
    end.

%% Backpressure check: bound the writer's mailbox.
writer_overload(Writer) ->
    case erlang:process_info(Writer, message_queue_len) of
        {message_queue_len, N} when N >= ?ASYNC_WRITER_MAX_QUEUE -> true;
        _ -> false
    end.

%% Per-fd writer process. Owns one async pipe write fd; its mailbox is
%% the implicit serialisation point for all responses on that fd.
%% Exits on hard write errors so the coordinator's 'DOWN' clause can
%% drop the entry; the next callback respawns it.
async_writer_loop(WriteFd) ->
    receive
        {respond, CallbackId, Response} ->
            case py_nif:async_callback_response(WriteFd, CallbackId, Response) of
                ok -> async_writer_loop(WriteFd);
                {error, _} = Err ->
                    exit({async_callback_response_failed, Err})
            end;
        {overflow, CallbackId} ->
            Response = <<1, "async callback queue full">>,
            case py_nif:async_callback_response(WriteFd, CallbackId, Response) of
                ok -> async_writer_loop(WriteFd);
                {error, _} = Err ->
                    exit({async_callback_response_failed, Err})
            end;
        shutdown ->
            ok;
        _Other ->
            async_writer_loop(WriteFd)
    end.

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
