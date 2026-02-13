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
            %% Spawn a separate callback handler process
            CallbackHandler = spawn_link(fun() -> callback_handler_loop() end),
            %% Set up callback handler with the separate process
            case py_nif:set_callback_handler(WorkerRef, CallbackHandler) of
                {ok, CallbackFd} ->
                    CallbackHandler ! {set_fd, CallbackFd},
                    Parent ! {self(), ready},
                    loop(WorkerRef, Parent, CallbackFd);
                {error, Reason} ->
                    exit(CallbackHandler, kill),
                    Parent ! {self(), {error, Reason}}
            end;
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

%% Separate process that handles callbacks from Python
callback_handler_loop() ->
    receive
        {set_fd, CallbackFd} ->
            callback_handler_loop(CallbackFd)
    end.

callback_handler_loop(CallbackFd) ->
    receive
        {erlang_callback, _CallbackId, FuncName, Args} ->
            handle_callback(CallbackFd, FuncName, Args),
            callback_handler_loop(CallbackFd);
        shutdown ->
            ok;
        _Other ->
            callback_handler_loop(CallbackFd)
    end.

loop(WorkerRef, _Parent, _CallbackFd) ->
    receive
        {py_request, Request} ->
            handle_request(WorkerRef, Request),
            loop(WorkerRef, _Parent, _CallbackFd);

        shutdown ->
            py_nif:worker_destroy(WorkerRef),
            ok;

        _Other ->
            loop(WorkerRef, _Parent, _CallbackFd)
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
%%% Callback Handling
%%% ============================================================================

handle_callback(CallbackFd, FuncName, Args) ->
    %% Convert Args from tuple to list if needed
    ArgsList = case Args of
        T when is_tuple(T) -> tuple_to_list(T);
        L when is_list(L) -> L;
        _ -> [Args]
    end,
    %% Execute the registered function
    case py_callback:execute(FuncName, ArgsList) of
        {ok, Result} ->
            %% Encode result as Python-parseable string
            %% Format: status_byte (0=ok) + python_repr
            ResultStr = term_to_python_repr(Result),
            Response = <<0, ResultStr/binary>>,
            py_nif:send_callback_response(CallbackFd, Response);
        {error, {not_found, Name}} ->
            ErrMsg = iolist_to_binary(io_lib:format("Function '~s' not registered", [Name])),
            Response = <<1, ErrMsg/binary>>,
            py_nif:send_callback_response(CallbackFd, Response);
        {error, {Class, Reason, _Stack}} ->
            ErrMsg = iolist_to_binary(io_lib:format("~p: ~p", [Class, Reason])),
            Response = <<1, ErrMsg/binary>>,
            py_nif:send_callback_response(CallbackFd, Response)
    end.

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
