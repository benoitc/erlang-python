%%% @doc Callback registry for Python -> Erlang function calls.
%%%
%%% This module manages registered Erlang functions that can be called
%%% from Python code via the `erlang.call()` function.
%%%
%%% @private
-module(py_callback).

-behaviour(gen_server).

-export([
    start_link/0,
    register/2,
    unregister/1,
    lookup/1,
    execute/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TABLE, py_callbacks).

%%% ============================================================================
%%% API
%%% ============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register a function to be callable from Python.
-spec register(Name :: atom() | binary(), Fun :: fun((list()) -> term()) | {atom(), atom()}) -> ok.
register(Name, Fun) ->
    NameBin = to_binary(Name),
    ets:insert(?TABLE, {NameBin, Fun}),
    ok.

%% @doc Unregister a function.
-spec unregister(Name :: atom() | binary()) -> ok.
unregister(Name) ->
    NameBin = to_binary(Name),
    ets:delete(?TABLE, NameBin),
    ok.

%% @doc Lookup a registered function.
-spec lookup(Name :: binary()) -> {ok, fun((list()) -> term()) | {atom(), atom()}} | {error, not_found}.
lookup(Name) ->
    NameBin = to_binary(Name),
    case ets:lookup(?TABLE, NameBin) of
        [{_, Fun}] -> {ok, Fun};
        [] -> {error, not_found}
    end.

%% @doc Execute a registered function with arguments.
-spec execute(Name :: binary(), Args :: list()) -> {ok, term()} | {error, term()}.
execute(Name, Args) ->
    case lookup(Name) of
        {ok, Fun} when is_function(Fun, 1) ->
            try
                Result = Fun(Args),
                {ok, Result}
            catch
                Class:Reason:Stack ->
                    {error, {Class, Reason, Stack}}
            end;
        {ok, {Module, Function}} ->
            try
                Result = apply(Module, Function, [Args]),
                {ok, Result}
            catch
                Class:Reason:Stack ->
                    {error, {Class, Reason, Stack}}
            end;
        {error, not_found} ->
            {error, {not_found, Name}}
    end.

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([]) ->
    %% Create ETS table for registered functions
    ?TABLE = ets:new(?TABLE, [
        named_table,
        public,
        set,
        {read_concurrency, true}
    ]),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Bin) when is_binary(Bin) ->
    Bin.
