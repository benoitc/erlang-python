%%% @doc Python worker process.
%%%
%%% Each worker maintains its own Python execution context. Workers
%%% receive requests from the pool and execute Python code, sending
%%% results back to callers.
%%%
%%% The NIF functions use ERL_NIF_DIRTY_JOB_IO_BOUND to run on dirty
%%% I/O schedulers, so the worker process itself runs on a normal scheduler.
%%%
%%% @private
-module(py_worker).

-export([
    start_link/0,
    init/1
]).

%% Timeout for checking shutdown (ms)
-define(RECV_TIMEOUT, 1000).

%%% ============================================================================
%%% API
%%% ============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    Pid = spawn_link(?MODULE, init, [self()]),
    receive
        {Pid, ready} -> {ok, Pid};
        {Pid, {error, Reason}} -> {error, Reason}
    after 10000 ->
        exit(Pid, kill),
        {error, timeout}
    end.

%%% ============================================================================
%%% Worker Process
%%% ============================================================================

init(Parent) ->
    %% Create worker context
    case py_nif:worker_new() of
        {ok, WorkerRef} ->
            Parent ! {self(), ready},
            loop(WorkerRef, Parent);
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

loop(WorkerRef, _Parent) ->
    receive
        {py_request, Request} ->
            handle_request(WorkerRef, Request),
            loop(WorkerRef, _Parent);

        shutdown ->
            py_nif:worker_destroy(WorkerRef),
            ok;

        _Other ->
            loop(WorkerRef, _Parent)
    end.

%%% ============================================================================
%%% Request Handling
%%% ============================================================================

%% Call with timeout
handle_request(WorkerRef, {call, Ref, Caller, Module, Func, Args, Kwargs, TimeoutMs}) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Result = py_nif:worker_call(WorkerRef, ModuleBin, FuncBin, Args, Kwargs, TimeoutMs),
    send_response(Caller, Ref, Result);

%% Call without timeout (backward compatible)
handle_request(WorkerRef, {call, Ref, Caller, Module, Func, Args, Kwargs}) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Result = py_nif:worker_call(WorkerRef, ModuleBin, FuncBin, Args, Kwargs),
    send_response(Caller, Ref, Result);

%% Eval with timeout
handle_request(WorkerRef, {eval, Ref, Caller, Code, Locals, TimeoutMs}) ->
    CodeBin = to_binary(Code),
    Result = py_nif:worker_eval(WorkerRef, CodeBin, Locals, TimeoutMs),
    send_response(Caller, Ref, Result);

%% Eval without timeout (backward compatible)
handle_request(WorkerRef, {eval, Ref, Caller, Code, Locals}) ->
    CodeBin = to_binary(Code),
    Result = py_nif:worker_eval(WorkerRef, CodeBin, Locals),
    send_response(Caller, Ref, Result);

handle_request(WorkerRef, {exec, Ref, Caller, Code}) ->
    CodeBin = to_binary(Code),
    Result = py_nif:worker_exec(WorkerRef, CodeBin),
    send_response(Caller, Ref, Result);

handle_request(WorkerRef, {stream, Ref, Caller, Module, Func, Args, Kwargs}) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    %% For streaming, we call a special function that yields chunks
    case py_nif:worker_call(WorkerRef, ModuleBin, FuncBin, Args, Kwargs) of
        {ok, {generator, GenRef}} ->
            stream_chunks(WorkerRef, GenRef, Ref, Caller);
        {ok, Value} ->
            %% Not a generator, send as single chunk
            Caller ! {py_chunk, Ref, Value},
            Caller ! {py_end, Ref};
        {error, _} = Error ->
            Caller ! {py_error, Ref, Error}
    end;

handle_request(WorkerRef, {stream_eval, Ref, Caller, Code, Locals}) ->
    %% Evaluate expression and stream if result is a generator
    CodeBin = to_binary(Code),
    case py_nif:worker_eval(WorkerRef, CodeBin, Locals) of
        {ok, {generator, GenRef}} ->
            stream_chunks(WorkerRef, GenRef, Ref, Caller);
        {ok, Value} ->
            %% Not a generator, send as single value
            Caller ! {py_chunk, Ref, Value},
            Caller ! {py_end, Ref};
        {error, _} = Error ->
            Caller ! {py_error, Ref, Error}
    end.

stream_chunks(WorkerRef, GenRef, Ref, Caller) ->
    case py_nif:worker_next(WorkerRef, GenRef) of
        {ok, {generator, NestedGen}} ->
            %% Nested generator - stream it inline
            stream_chunks(WorkerRef, NestedGen, Ref, Caller);
        {ok, Chunk} ->
            Caller ! {py_chunk, Ref, Chunk},
            stream_chunks(WorkerRef, GenRef, Ref, Caller);
        {error, stop_iteration} ->
            Caller ! {py_end, Ref};
        {error, Error} ->
            Caller ! {py_error, Ref, Error}
    end.

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

send_response(Caller, Ref, {ok, Value}) ->
    Caller ! {py_response, Ref, {ok, Value}};
send_response(Caller, Ref, {error, Error}) ->
    Caller ! {py_error, Ref, Error};
send_response(Caller, Ref, ok) ->
    Caller ! {py_response, Ref, {ok, none}}.

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
