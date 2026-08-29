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

%%% @doc Streaming results from Python generators into Erlang messages.
%%% Implementation of `py:stream/3,4', `py:stream_eval/1,2', `py:stream_start/3,4'
%%% and `py:stream_cancel/1'; use those. Owns the `{py_stream, Ref, ...}'
%%% event protocol and the Python-side generator driver.
%%% @private
-module(py_stream).

-export([
    stream/3,
    stream/4,
    stream_eval/1,
    stream_eval/2,
    stream_start/3,
    stream_start/4,
    stream_cancel/1
]).


%% @doc Stream results from a Python generator.
%% Returns a list of all yielded values.
-spec stream(py:py_module(), py:py_func(), py:py_args()) -> py:py_result().
stream(Module, Func, Args) ->
    stream(Module, Func, Args, #{}).

%% @doc Stream results from a Python generator with kwargs.
-spec stream(py:py_module(), py:py_func(), py:py_args(), py:py_kwargs()) -> py:py_result().
stream(Module, Func, Args, Kwargs) when map_size(Kwargs) == 0 ->
    %% No kwargs - use stream_start and collect results
    {ok, Ref} = stream_start(Module, Func, Args),
    collect_stream(Ref, []);
stream(Module, Func, Args, Kwargs) ->
    %% With kwargs - use eval approach
    Ctx = py_context_router:get_context(),
    ModuleBin = py_util:valid_py_module(py_util:to_binary(Module)),
    FuncBin = py_util:valid_py_ident(py_util:to_binary(Func)),
    KwargsCode = format_kwargs(Kwargs),
    ArgsCode = format_args(Args),
    Code = iolist_to_binary([
        <<"list(__import__('">>, ModuleBin, <<"').">>, FuncBin,
        <<"(">>, ArgsCode, KwargsCode, <<"))">>
    ]),
    py_context:eval(Ctx, Code, #{}).

%% @private Collect all stream events into a list
collect_stream(Ref, Acc) ->
    receive
        {py_stream, Ref, {data, Value}} ->
            collect_stream(Ref, [Value | Acc]);
        {py_stream, Ref, done} ->
            {ok, lists:reverse(Acc)};
        {py_stream, Ref, {error, Reason}} ->
            {error, Reason}
    after 30000 ->
        {error, timeout}
    end.

%% @private Format arguments for Python code
format_args([]) -> <<>>;
format_args(Args) ->
    ArgStrs = [format_arg(A) || A <- Args],
    iolist_to_binary(lists:join(<<", ">>, ArgStrs)).

%% @private Format a single argument
format_arg(A) when is_integer(A) -> integer_to_binary(A);
format_arg(A) when is_float(A) -> float_to_binary(A);
format_arg(A) when is_binary(A) -> <<"'", (py_util:escape_py_literal(A))/binary, "'">>;
format_arg(A) when is_atom(A) -> <<"'", (py_util:escape_py_literal(atom_to_binary(A)))/binary, "'">>;
format_arg(A) when is_list(A) -> iolist_to_binary([<<"[">>, format_args(A), <<"]">>]);
format_arg(_) -> <<"None">>.

%% @private Format kwargs for Python code
format_kwargs(Kwargs) when map_size(Kwargs) == 0 -> <<>>;
format_kwargs(Kwargs) ->
    KwList = maps:fold(fun(K, V, Acc) ->
        KB = py_util:valid_py_ident(if is_atom(K) -> atom_to_binary(K); is_binary(K) -> K end),
        [<<KB/binary, "=", (format_arg(V))/binary>> | Acc]
    end, [], Kwargs),
    iolist_to_binary([<<", ">>, lists:join(<<", ">>, KwList)]).

%% @doc Stream results from a Python generator expression.
%% Evaluates the expression and if it returns a generator, streams all values.
-spec stream_eval(string() | binary()) -> py:py_result().
stream_eval(Code) ->
    stream_eval(Code, #{}).

%% @doc Stream results from a Python generator expression with local variables.
-spec stream_eval(string() | binary(), map()) -> py:py_result().
stream_eval(Code, Locals) ->
    %% Route through the new process-per-context system
    %% Wrap the code in list() to collect generator values
    Ctx = py_context_router:get_context(),
    CodeBin = py_util:to_binary(Code),
    WrappedCode = <<"list(", CodeBin/binary, ")">>,
    py_context:eval(Ctx, WrappedCode, Locals).

%%% ============================================================================
%%% True Streaming API (Event-driven)
%%% ============================================================================

%% @doc Start a true streaming iteration from a Python generator.
%%
%% Unlike stream/3,4 which collects all values at once, this function
%% returns immediately with a reference and sends values as events
%% to the calling process as they are yielded.
%%
%% Events sent to the owner process:
%% - `{py_stream, Ref, {data, Value}}' - Each yielded value
%% - `{py_stream, Ref, done}' - Stream completed
%% - `{py_stream, Ref, {error, Reason}}' - Stream error
%%
%% Accepts sync generators and async generators. An async generator is driven
%% on a private event loop, one value at a time; delivering a value blocks that
%% loop, so other coroutines on it do not progress between yields.
%%
%% Example:
%% ```
%% {ok, Ref} = py:stream_start(builtins, iter, [[1,2,3,4,5]]),
%% receive_loop(Ref).
%%
%% receive_loop(Ref) ->
%%     receive
%%         {py_stream, Ref, {data, Value}} ->
%%             io:format("Got: ~p~n", [Value]),
%%             receive_loop(Ref);
%%         {py_stream, Ref, done} ->
%%             io:format("Complete~n");
%%         {py_stream, Ref, {error, Reason}} ->
%%             io:format("Error: ~p~n", [Reason])
%%     after 30000 ->
%%         timeout
%%     end.
%% '''
-spec stream_start(py:py_module(), py:py_func(), py:py_args()) -> {ok, reference()}.
stream_start(Module, Func, Args) ->
    stream_start(Module, Func, Args, #{}).

%% @doc Start a true streaming iteration with options.
%%
%% Options:
%% - `owner => pid()' - Process to receive events (default: self())
%%
%% @param Module Python module name
%% @param Func Python function name
%% @param Args Function arguments
%% @param Opts Options map
%% @returns {ok, Ref} where Ref is used to identify stream events
-spec stream_start(py:py_module(), py:py_func(), py:py_args(), map()) -> {ok, reference()}.
stream_start(Module, Func, Args, Opts) ->
    Owner = maps:get(owner, Opts, self()),
    Ref = make_ref(),
    ModuleBin = py_util:to_binary(Module),
    FuncBin = py_util:to_binary(Func),
    RefHash = erlang:phash2(Ref),
    %% Store owner and ref for Python to retrieve
    %% Use binary keys because Python strings become binaries
    py_state:store({<<"stream_owner">>, RefHash}, Owner),
    py_state:store({<<"stream_ref">>, RefHash}, Ref),
    py_state:store({<<"stream_args">>, RefHash}, Args),
    %% Spawn an Erlang process to run the streaming iteration
    spawn(fun() ->
        stream_run_python(ModuleBin, FuncBin, RefHash)
    end),
    {ok, Ref}.

%% @private Run the streaming via Python code
stream_run_python(ModuleBin0, FuncBin0, RefHash) ->
    ModuleBin = py_util:valid_py_module(ModuleBin0),
    FuncBin = py_util:valid_py_ident(FuncBin0),
    RefHashBin = integer_to_binary(RefHash),
    %% Build Python code that streams values using callbacks
    Code = iolist_to_binary([
        <<"import erlang\n">>,
        <<"_rh = ">>, RefHashBin, <<"\n">>,
        <<"_args = erlang.call('state_get', ('stream_args', _rh))\n">>,
        <<"if _args is None:\n">>,
        <<"    _args = []\n">>,
        <<"try:\n">>,
        <<"    _mod = __import__('">>, ModuleBin, <<"')\n">>,
        <<"    _fn = getattr(_mod, '">>, FuncBin, <<"')\n">>,
        <<"    _gen = _fn(*_args) if _args else _fn()\n">>,
        %% Async generators are driven on a private event loop. erlang.call is
        %% a blocking pipe read, so it stalls that loop between yields, which
        %% is fine for a sequential stream.
        <<"    if hasattr(_gen, '__anext__'):\n">>,
        <<"        import asyncio\n">>,
        <<"        async def _drive():\n">>,
        <<"            async for _val in _gen:\n">>,
        <<"                if erlang.call('_py_stream_cancelled', _rh):\n">>,
        <<"                    erlang.call('_py_stream_send', _rh, 'error', 'cancelled')\n">>,
        <<"                    return\n">>,
        <<"                erlang.call('_py_stream_send', _rh, 'data', _val)\n">>,
        <<"            erlang.call('_py_stream_send', _rh, 'done', None)\n">>,
        <<"        asyncio.run(_drive())\n">>,
        <<"    else:\n">>,
        <<"        for _val in _gen:\n">>,
        <<"            if erlang.call('_py_stream_cancelled', _rh):\n">>,
        <<"                erlang.call('_py_stream_send', _rh, 'error', 'cancelled')\n">>,
        <<"                break\n">>,
        <<"            erlang.call('_py_stream_send', _rh, 'data', _val)\n">>,
        <<"        else:\n">>,
        <<"            erlang.call('_py_stream_send', _rh, 'done', None)\n">>,
        <<"except Exception as _e:\n">>,
        <<"    erlang.call('_py_stream_send', _rh, 'error', str(_e))\n">>,
        <<"finally:\n">>,
        <<"    erlang.call('_py_stream_cleanup', _rh)\n">>
    ]),
    %% Execute the streaming code
    case py:exec(Code) of
        ok -> ok;
        {error, Reason} ->
            %% Try to notify owner of error
            case py_state:fetch({<<"stream_owner">>, RefHash}) of
                {ok, Owner} ->
                    case py_state:fetch({<<"stream_ref">>, RefHash}) of
                        {ok, Ref} ->
                            Owner ! {py_stream, Ref, {error, Reason}},
                            py_state:remove({<<"stream_owner">>, RefHash}),
                            py_state:remove({<<"stream_ref">>, RefHash}),
                            py_state:remove({<<"stream_args">>, RefHash});
                        _ -> ok
                    end;
                _ -> ok
            end
    end.

%% @doc Cancel an active stream.
%%
%% Sends a cancellation signal to stop the stream iteration.
%% Any pending values may still be delivered before the stream stops.
%%
%% @param Ref The stream reference from stream_start/3,4
%% @returns ok
-spec stream_cancel(reference()) -> ok.
stream_cancel(Ref) when is_reference(Ref) ->
    %% Store cancellation flag that the streaming task checks
    %% Use hash because we can't pass Erlang refs to Python callbacks easily
    %% Use binary key because Python strings become binaries
    RefHash = erlang:phash2(Ref),
    py_state:store({<<"stream_cancelled_hash">>, RefHash}, true),
    ok.

