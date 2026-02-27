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

%%% @doc Python context process.
%%%
%%% A py_context process owns a Python context (subinterpreter or worker).
%%% Each process has exclusive access to its context, eliminating mutex
%%% contention and enabling true N-way parallelism.
%%%
%%% The context is created when the process starts and destroyed when it
%%% stops. All Python operations are serialized through message passing.
%%%
%%% == Callback Handling ==
%%%
%%% When Python code calls `erlang.call()`, the NIF returns a `{suspended, ...}`
%%% tuple instead of blocking. The context process handles the callback inline
%%% using a recursive receive pattern, enabling arbitrarily deep callback nesting.
%%%
%%% This approach is inspired by PyO3's suspension mechanism and avoids the
%%% deadlock issues that occur with separate callback handler processes.
%%%
%%% @end
-module(py_context).

-export([
    start_link/2,
    stop/1,
    call/5,
    call/6,
    eval/3,
    eval/4,
    exec/2,
    call_method/4,
    to_term/1,
    get_interp_id/1
]).

%% Internal exports
-export([init/3]).

-type context_mode() :: auto | subinterp | worker.
-type context() :: pid().

-export_type([context_mode/0, context/0]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a new py_context process.
%%
%% The process creates a Python context based on the mode:
%% - `auto' - Detect best mode (subinterp on Python 3.12+, worker otherwise)
%% - `subinterp' - Create a sub-interpreter with its own GIL
%% - `worker' - Create a thread-state worker
%%
%% @param Id Unique identifier for this context
%% @param Mode Context mode
%% @returns {ok, Pid} | {error, Reason}
-spec start_link(pos_integer(), context_mode()) -> {ok, pid()} | {error, term()}.
start_link(Id, Mode) ->
    Parent = self(),
    Pid = spawn_link(fun() -> init(Parent, Id, Mode) end),
    receive
        {Pid, started} ->
            {ok, Pid};
        {Pid, {error, Reason}} ->
            {error, Reason}
    after 5000 ->
        exit(Pid, kill),
        {error, timeout}
    end.

%% @doc Stop a py_context process.
-spec stop(context()) -> ok.
stop(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {stop, self(), MRef},
    receive
        {MRef, ok} ->
            erlang:demonitor(MRef, [flush]),
            ok;
        {'DOWN', MRef, process, Ctx, _Reason} ->
            ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        exit(Ctx, kill),
        ok
    end.

%% @doc Call a Python function.
%%
%% @param Ctx Context process
%% @param Module Python module name
%% @param Func Function name
%% @param Args List of arguments
%% @param Kwargs Map of keyword arguments
%% @returns {ok, Result} | {error, Reason}
-spec call(context(), atom() | binary(), atom() | binary(), list(), map()) ->
    {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args, Kwargs) ->
    call(Ctx, Module, Func, Args, Kwargs, infinity).

%% @doc Call a Python function with timeout.
-spec call(context(), atom() | binary(), atom() | binary(), list(), map(),
           timeout()) -> {ok, term()} | {error, term()}.
call(Ctx, Module, Func, Args, Kwargs, Timeout) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    Ctx ! {call, self(), MRef, ModuleBin, FuncBin, Args, Kwargs},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Evaluate a Python expression.
%%
%% @param Ctx Context process
%% @param Code Python code to evaluate
%% @param Locals Map of local variables
%% @returns {ok, Result} | {error, Reason}
-spec eval(context(), binary() | string(), map()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code, Locals) ->
    eval(Ctx, Code, Locals, infinity).

%% @doc Evaluate a Python expression with timeout.
-spec eval(context(), binary() | string(), map(), timeout()) ->
    {ok, term()} | {error, term()}.
eval(Ctx, Code, Locals, Timeout) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {eval, self(), MRef, CodeBin, Locals},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after Timeout ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Execute Python statements.
%%
%% @param Ctx Context process
%% @param Code Python code to execute
%% @returns ok | {error, Reason}
-spec exec(context(), binary() | string()) -> ok | {error, term()}.
exec(Ctx, Code) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    CodeBin = to_binary(Code),
    Ctx ! {exec, self(), MRef, CodeBin},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    after infinity ->
        erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

%% @doc Call a method on a Python object reference.
-spec call_method(context(), reference(), atom() | binary(), list()) ->
    {ok, term()} | {error, term()}.
call_method(Ctx, Ref, Method, Args) when is_pid(Ctx), is_reference(Ref) ->
    MRef = erlang:monitor(process, Ctx),
    MethodBin = to_binary(Method),
    Ctx ! {call_method, self(), MRef, Ref, MethodBin, Args},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    end.

%% @doc Convert a Python object reference to an Erlang term.
-spec to_term(reference()) -> {ok, term()} | {error, term()}.
to_term(Ref) when is_reference(Ref) ->
    %% This uses the ref's embedded interp_id to route automatically
    py_nif:context_to_term(Ref).

%% @doc Get the interpreter ID for this context.
-spec get_interp_id(context()) -> {ok, non_neg_integer()} | {error, term()}.
get_interp_id(Ctx) when is_pid(Ctx) ->
    MRef = erlang:monitor(process, Ctx),
    Ctx ! {get_interp_id, self(), MRef},
    receive
        {MRef, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Ctx, Reason} ->
            {error, {context_died, Reason}}
    end.

%% ============================================================================
%% Internal functions
%% ============================================================================

%% @private
init(Parent, Id, Mode) ->
    case create_context(Mode) of
        {ok, Ref, InterpId} ->
            %% No callback handler process needed - we handle callbacks inline
            %% using the suspension-based approach with recursive receive
            Parent ! {self(), started},
            loop(Ref, Id, InterpId);
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

%% @private
create_context(auto) ->
    case py_nif:subinterp_supported() of
        true -> create_context(subinterp);
        false -> create_context(worker)
    end;
create_context(subinterp) ->
    py_nif:context_create(subinterp);
create_context(worker) ->
    py_nif:context_create(worker).

%% @private
%% Main context loop. Handles requests and uses suspension-based callback support.
loop(Ref, Id, InterpId) ->
    receive
        {call, From, MRef, Module, Func, Args, Kwargs} ->
            Result = handle_call_with_suspension(Ref, Module, Func, Args, Kwargs),
            From ! {MRef, Result},
            loop(Ref, Id, InterpId);

        {eval, From, MRef, Code, Locals} ->
            Result = handle_eval_with_suspension(Ref, Code, Locals),
            From ! {MRef, Result},
            loop(Ref, Id, InterpId);

        {exec, From, MRef, Code} ->
            Result = py_nif:context_exec(Ref, Code),
            From ! {MRef, Result},
            loop(Ref, Id, InterpId);

        {call_method, From, MRef, ObjRef, Method, Args} ->
            Result = py_nif:context_call_method(Ref, ObjRef, Method, Args),
            From ! {MRef, Result},
            loop(Ref, Id, InterpId);

        {get_interp_id, From, MRef} ->
            From ! {MRef, {ok, InterpId}},
            loop(Ref, Id, InterpId);

        {stop, From, MRef} ->
            destroy_context(Ref),
            From ! {MRef, ok}
    end.

%% ============================================================================
%% Suspension-based callback handling
%% ============================================================================
%%
%% When Python calls erlang.call(), the NIF returns {suspended, ...} instead of
%% blocking. We handle the callback inline and then resume Python execution.
%% This enables unlimited nesting depth without deadlock.

%% @private
%% Handle call with potential suspension for callbacks
handle_call_with_suspension(Ref, Module, Func, Args, Kwargs) ->
    case py_nif:context_call(Ref, Module, Func, Args, Kwargs) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            %% Callback needed - handle it with recursive receive
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            %% Resume and potentially get more suspensions
            resume_and_continue(Ref, StateRef, CallbackResult);
        Result ->
            Result
    end.

%% @private
%% Handle eval with potential suspension for callbacks
handle_eval_with_suspension(Ref, Code, Locals) ->
    case py_nif:context_eval(Ref, Code, Locals) of
        {suspended, _CallbackId, StateRef, {FuncName, CallbackArgs}} ->
            %% Callback needed - handle it with recursive receive
            CallbackResult = handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs),
            %% Resume and potentially get more suspensions
            resume_and_continue(Ref, StateRef, CallbackResult);
        Result ->
            Result
    end.

%% @private
%% Handle callback, allowing nested py:eval/call to be processed.
%% We spawn a process to execute the callback so we can stay in a receive loop
%% for nested calls while the callback runs.
handle_callback_with_nested_receive(Ref, FuncName, CallbackArgs) ->
    Parent = self(),
    CallbackPid = spawn_link(fun() ->
        Result = try
            ArgsList = tuple_to_list(CallbackArgs),
            case py_callback:execute(FuncName, ArgsList) of
                {ok, Value} ->
                    ReprStr = term_to_python_repr(Value),
                    {ok, <<0, ReprStr/binary>>};
                {error, Reason} ->
                    ErrMsg = iolist_to_binary(io_lib:format("~p", [Reason])),
                    {ok, <<1, ErrMsg/binary>>}
            end
        catch
            Class:ExcReason:Stacktrace ->
                ErrorMsg = iolist_to_binary(io_lib:format("~p:~p~n~p",
                    [Class, ExcReason, Stacktrace])),
                {ok, <<1, ErrorMsg/binary>>}
        end,
        Parent ! {callback_result, self(), Result}
    end),
    %% Wait for callback, processing nested requests
    wait_for_callback(Ref, CallbackPid).

%% @private
%% Wait for callback result while processing nested py:call/eval requests.
%% This enables arbitrarily deep callback nesting.
wait_for_callback(Ref, CallbackPid) ->
    receive
        {callback_result, CallbackPid, Result} ->
            Result;

        %% Handle nested py:call while waiting for callback
        {call, From, MRef, Module, Func, Args, Kwargs} ->
            NestedResult = handle_call_with_suspension(Ref, Module, Func, Args, Kwargs),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:eval while waiting for callback
        {eval, From, MRef, Code, Locals} ->
            NestedResult = handle_eval_with_suspension(Ref, Code, Locals),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested py:exec while waiting for callback
        {exec, From, MRef, Code} ->
            NestedResult = py_nif:context_exec(Ref, Code),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle nested call_method while waiting for callback
        {call_method, From, MRef, ObjRef, Method, Args} ->
            NestedResult = py_nif:context_call_method(Ref, ObjRef, Method, Args),
            From ! {MRef, NestedResult},
            wait_for_callback(Ref, CallbackPid);

        %% Handle get_interp_id while waiting
        {get_interp_id, From, MRef} ->
            %% We can't get InterpId here, but we can query the NIF
            InterpIdResult = py_nif:context_interp_id(Ref),
            From ! {MRef, InterpIdResult},
            wait_for_callback(Ref, CallbackPid)
    end.

%% @private
%% Resume suspended state, handle additional suspensions (nested callbacks)
resume_and_continue(Ref, StateRef, {ok, ResultBin}) ->
    case py_nif:context_resume(Ref, StateRef, ResultBin) of
        {suspended, _CallbackId2, StateRef2, {FuncName2, Args2}} ->
            %% Another callback during resume - recursive handling
            CallbackResult2 = handle_callback_with_nested_receive(Ref, FuncName2, Args2),
            resume_and_continue(Ref, StateRef2, CallbackResult2);
        FinalResult ->
            FinalResult
    end;
resume_and_continue(Ref, StateRef, {error, _} = Err) ->
    _ = py_nif:context_cancel_resume(Ref, StateRef),
    Err.

%% ============================================================================
%% Utility functions
%% ============================================================================

%% @private
%% Convert Erlang term to Python repr string
term_to_python_repr(Term) when is_integer(Term) ->
    integer_to_binary(Term);
term_to_python_repr(Term) when is_float(Term) ->
    float_to_binary(Term, [{decimals, 15}, compact]);
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
    BinStr = atom_to_binary(Term, utf8),
    <<"'", BinStr/binary, "'">>;
term_to_python_repr(Term) when is_binary(Term) ->
    %% Escape the binary for Python
    Escaped = binary:replace(Term, <<"'">>, <<"\\'">>, [global]),
    <<"'", Escaped/binary, "'">>;
term_to_python_repr(Term) when is_list(Term) ->
    case io_lib:printable_unicode_list(Term) of
        true ->
            %% It's a string
            Bin = unicode:characters_to_binary(Term),
            Escaped = binary:replace(Bin, <<"'">>, <<"\\'">>, [global]),
            <<"'", Escaped/binary, "'">>;
        false ->
            %% It's a list
            Items = [term_to_python_repr(E) || E <- Term],
            ItemsBin = join_binaries(Items, <<", ">>),
            <<"[", ItemsBin/binary, "]">>
    end;
term_to_python_repr(Term) when is_tuple(Term) ->
    Items = [term_to_python_repr(E) || E <- tuple_to_list(Term)],
    ItemsBin = join_binaries(Items, <<", ">>),
    case tuple_size(Term) of
        1 -> <<"(", ItemsBin/binary, ",)">>;
        _ -> <<"(", ItemsBin/binary, ")">>
    end;
term_to_python_repr(Term) when is_map(Term) ->
    Items = maps:fold(fun(K, V, Acc) ->
        KeyRepr = term_to_python_repr(K),
        ValRepr = term_to_python_repr(V),
        [<<KeyRepr/binary, ": ", ValRepr/binary>> | Acc]
    end, [], Term),
    ItemsBin = join_binaries(lists:reverse(Items), <<", ">>),
    <<"{", ItemsBin/binary, "}">>;
term_to_python_repr(_Term) ->
    <<"None">>.

%% @private
join_binaries([], _Sep) -> <<>>;
join_binaries([H], _Sep) -> H;
join_binaries([H|T], Sep) ->
    lists:foldl(fun(B, Acc) -> <<Acc/binary, Sep/binary, B/binary>> end, H, T).

%% @private
destroy_context(Ref) ->
    py_nif:context_destroy(Ref).

%% @private
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
