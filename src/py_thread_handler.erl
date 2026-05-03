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
%%   async_writers  — async pipes: WriteFd => {WriterPid, Counter} where
%%                    Counter is an atomics ref tracking in-flight async
%%                    callbacks (incremented at submission, decremented
%%                    after the writer hands the frame to the NIF).
%%   writer_fds     — reverse map: WriterPid => WriteFd (fast lookup on
%%                    'DOWN' messages).
-record(state, {
    handlers      = #{} :: #{non_neg_integer() => {pid(), integer()}},
    async_writers = #{} :: #{integer() => {pid(), atomics:atomics_ref()}},
    writer_fds    = #{} :: #{pid() => integer()}
}).

%% Bound on combined async in-flight + writer-mailbox depth. The
%% atomics counter covers BOTH executors that have spawned but not
%% yet enqueued AND messages already sitting in the writer's mailbox,
%% so the bound holds even when many parallel callbacks complete
%% before the writer drains. Submissions beyond this point are failed
%% inline with an "async callback queue full" error frame.
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
    {Writer, Counter, State1} = ensure_async_writer(WriteFd, State0),
    %% Reserve a slot in the in-flight counter. atomics:add_get is
    %% atomic and gives us the post-increment value, so the check is
    %% race-free across submissions on different schedulers.
    case atomics:add_get(Counter, 1, 1) of
        N when N > ?ASYNC_WRITER_MAX_QUEUE ->
            %% Already at the bound: undo the reservation and fail
            %% this callback with a queue-full error frame. The
            %% writer still serialises the error frame on the pipe
            %% (so the Future resolves with a clear RuntimeError
            %% rather than hanging), and we use the same atomics ref
            %% so the queued overflow message stays accounted for
            %% until the writer drains it.
            atomics:sub(Counter, 1, 1),
            Writer ! {overflow, CallbackId, Counter};
        _ ->
            spawn(fun() ->
                Response = run_async_callback(FuncName, Args),
                Writer ! {respond, CallbackId, Response, Counter}
            end)
    end,
    {noreply, State1};

%% Linked-process exit: covers both sync handlers and async writers.
%% Both are spawn_link'd so a coordinator crash takes them with it
%% (no leaked writer holding the async pipe fd after the gen_server
%% restarts). When THEY die independently we trap the EXIT here and
%% remove the dead pid from whichever map it lives in.
handle_info({'EXIT', Pid, _Reason},
            #state{handlers      = Handlers,
                   async_writers = Writers,
                   writer_fds    = WriterFds} = State) ->
    NewHandlers = maps:filter(
        fun(_WorkerId, {HandlerPid, _Fd}) -> HandlerPid =/= Pid end,
        Handlers),
    case maps:take(Pid, WriterFds) of
        {WriteFd, NewWriterFds} ->
            NewWriters = maps:remove(WriteFd, Writers),
            {noreply, State#state{handlers      = NewHandlers,
                                  async_writers = NewWriters,
                                  writer_fds    = NewWriterFds}};
        error ->
            {noreply, State#state{handlers = NewHandlers}}
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
%% spawn_link'd from the coordinator (which traps exits): a writer
%% death surfaces as `{'EXIT', WriterPid, _}' for cleanup, and a
%% coordinator crash propagates the EXIT signal back to the writer
%% (which does not trap) so it dies with the gen_server instead of
%% leaking the pipe fd. Supervision then restarts the gen_server with
%% an empty `async_writers' map and writers are respawned lazily on
%% the next callback. Each writer gets its own atomics counter that
%% bounds total in-flight async callbacks (executors + queued
%% responses) on that pipe.
ensure_async_writer(WriteFd,
                    #state{async_writers = Writers,
                           writer_fds    = WriterFds} = State) ->
    case maps:get(WriteFd, Writers, undefined) of
        undefined ->
            Counter = atomics:new(1, [{signed, false}]),
            Writer = spawn_link(fun() -> async_writer_loop(WriteFd) end),
            {Writer, Counter, State#state{
                async_writers = Writers#{WriteFd => {Writer, Counter}},
                writer_fds    = WriterFds#{Writer => WriteFd}}};
        {Writer, Counter} ->
            {Writer, Counter, State}
    end.

%% Per-fd writer process. Owns one async pipe write fd and is the only
%% process that calls py_nif:async_callback_response/3 for it; its
%% mailbox is the serialisation point for all responses on that fd.
%%
%% The {respond, ...} message carries the per-pipe atomics counter
%% from ensure_async_writer/2. The writer ALWAYS decrements after
%% handing the frame to the NIF (success or error) so the in-flight
%% bound is honoured even when the underlying write fails. {overflow,
%% ...} messages do not carry the counter — the gen_server already
%% un-reserved before sending — and the writer just emits the
%% error frame.
%%
%% Lifecycle:
%%   - spawn_link'd from the coordinator: a coordinator crash
%%     propagates an unhandled EXIT here (we do NOT trap_exit) and
%%     the writer dies with it, releasing the fd before supervision
%%     restarts the gen_server.
%%   - On hard write errors the writer exits normally. The
%%     coordinator's trap_exit clause catches the EXIT, drops the
%%     map entry, and the next callback respawns the writer with a
%%     fresh atomics counter so any leaked count from the previous
%%     instance is discarded with the dead writer.
async_writer_loop(WriteFd) ->
    receive
        {respond, CallbackId, Response, Counter} ->
            R = py_nif:async_callback_response(WriteFd, CallbackId, Response),
            atomics:sub(Counter, 1, 1),
            case R of
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
