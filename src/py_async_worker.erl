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

%%% @doc Async Python worker process with background event loop.
%%%
%%% Each async worker maintains a background thread running an asyncio
%%% event loop. Coroutines are submitted to this loop and results are
%%% delivered as Erlang messages.
%%%
%%% @private
-module(py_async_worker).

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
    %% Create async worker context with event loop
    case py_nif:async_worker_new() of
        {ok, WorkerRef} ->
            Parent ! {self(), ready},
            loop(WorkerRef, Parent, #{});
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

loop(WorkerRef, Parent, Pending) ->
    receive
        {py_async_request, Request} ->
            NewPending = handle_request(WorkerRef, Request, Pending),
            loop(WorkerRef, Parent, NewPending);

        {async_result, AsyncId, Result} ->
            %% Forward result to caller if we have them registered
            case maps:get(AsyncId, Pending, undefined) of
                undefined ->
                    loop(WorkerRef, Parent, Pending);
                {Ref, Caller} ->
                    send_response(Caller, Ref, Result),
                    loop(WorkerRef, Parent, maps:remove(AsyncId, Pending))
            end;

        shutdown ->
            py_nif:async_worker_destroy(WorkerRef),
            ok;

        _Other ->
            loop(WorkerRef, Parent, Pending)
    end.

%%% ============================================================================
%%% Request Handling
%%% ============================================================================

%% Async call
handle_request(WorkerRef, {async_call, Ref, Caller, Module, Func, Args, Kwargs}, Pending) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    case py_nif:async_call(WorkerRef, ModuleBin, FuncBin, Args, Kwargs, self()) of
        {ok, {immediate, Result}} ->
            %% Not a coroutine - result is available immediately
            send_response(Caller, Ref, {ok, Result}),
            Pending;
        {ok, AsyncId} ->
            %% Coroutine submitted - register for callback
            maps:put(AsyncId, {Ref, Caller}, Pending);
        {error, _} = Error ->
            Caller ! {py_error, Ref, Error},
            Pending
    end;

%% Async gather
handle_request(WorkerRef, {async_gather, Ref, Caller, Calls}, Pending) ->
    %% Convert calls to binary format
    BinCalls = [{to_binary(M), to_binary(F), A} || {M, F, A} <- Calls],
    case py_nif:async_gather(WorkerRef, BinCalls, self()) of
        {ok, {immediate, Results}} ->
            send_response(Caller, Ref, {ok, Results}),
            Pending;
        {ok, AsyncId} ->
            maps:put(AsyncId, {Ref, Caller}, Pending);
        {error, _} = Error ->
            Caller ! {py_error, Ref, Error},
            Pending
    end;

%% Async stream
handle_request(WorkerRef, {async_stream, Ref, Caller, Module, Func, Args, Kwargs}, Pending) ->
    ModuleBin = to_binary(Module),
    FuncBin = to_binary(Func),
    case py_nif:async_stream(WorkerRef, ModuleBin, FuncBin, Args, Kwargs, self()) of
        {ok, AsyncId} ->
            maps:put(AsyncId, {Ref, Caller}, Pending);
        {error, _} = Error ->
            Caller ! {py_error, Ref, Error},
            Pending
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
