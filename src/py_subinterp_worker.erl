%%% @doc Sub-interpreter Python worker process.
%%%
%%% Each worker has its own Python sub-interpreter with its own GIL,
%%% allowing true parallel execution on Python 3.12+.
%%%
%%% @private
-module(py_subinterp_worker).

-export([
    start_link/0,
    init/1
]).

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
    %% Create sub-interpreter worker with its own GIL
    case py_nif:subinterp_worker_new() of
        {ok, WorkerRef} ->
            Parent ! {self(), ready},
            loop(WorkerRef, Parent);
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

loop(WorkerRef, Parent) ->
    receive
        {py_subinterp_request, Request} ->
            handle_request(WorkerRef, Request),
            loop(WorkerRef, Parent);

        shutdown ->
            py_nif:subinterp_worker_destroy(WorkerRef),
            ok;

        _Other ->
            loop(WorkerRef, Parent)
    end.

%%% ============================================================================
%%% Request Handling
%%% ============================================================================

handle_request(WorkerRef, {call, Ref, Caller, Module, Func, Args, Kwargs}) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Result = py_nif:subinterp_call(WorkerRef, ModuleBin, FuncBin, Args, Kwargs),
    send_response(Caller, Ref, Result).

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
