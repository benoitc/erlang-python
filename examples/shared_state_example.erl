#!/usr/bin/env escript
%%% @doc Shared state example - demonstrates sharing data between Python workers.
%%%
%%% Since each Python worker has its own namespace, functions and variables
%%% defined in one call aren't visible in another. The shared state API
%%% provides ETS-backed storage accessible from both Python and Erlang.
%%%
%%% Prerequisites: rebar3 compile
%%% Run from project root: escript examples/shared_state_example.erl

-mode(compile).

main(_) ->
    %% Add the compiled beam files to the code path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Shared State Example ===~n~n"),

    %% Clear any existing state
    py:state_clear(),

    %% Example 1: Set from Erlang, read from Python
    io:format("1. Erlang -> Python:~n"),
    py:state_store(<<"config">>, #{
        api_key => <<"secret123">>,
        max_retries => 3,
        timeout => 5000
    }),
    %% Python reads using idiomatic import syntax
    ok = py:exec(<<"
from erlang import state_get

config = state_get('config')
assert config['api_key'] == 'secret123'
assert config['max_retries'] == 3
">>),
    io:format("   Python read config successfully~n"),

    %% Example 2: Set from Python, read from Erlang
    io:format("~n2. Python -> Erlang:~n"),
    ok = py:exec(<<"
from erlang import state_set

state_set('computation_result', {
    'status': 'complete',
    'values': [1, 2, 3, 4, 5],
    'metadata': {'source': 'python', 'timestamp': 1234567890}
})
">>),
    {ok, Result} = py:state_fetch(<<"computation_result">>),
    io:format("   Result in Erlang: ~p~n", [Result]),

    %% Example 3: Atomic counters - thread-safe increment/decrement
    io:format("~n3. Atomic counters (thread-safe):~n"),

    %% From Erlang
    Val1 = py:state_incr(<<"hits">>),
    io:format("   Erlang incr: ~p~n", [Val1]),
    Val2 = py:state_incr(<<"hits">>, 10),
    io:format("   Erlang incr by 10: ~p~n", [Val2]),

    %% From Python
    ok = py:exec(<<"
from erlang import state_incr, state_decr

val = state_incr('hits', 5)
print(f'   Python incr by 5: {val}')

val = state_decr('hits', 3)
print(f'   Python decr by 3: {val}')
">>),

    {ok, FinalCount} = py:state_fetch(<<"hits">>),
    io:format("   Final counter value: ~p~n", [FinalCount]),

    %% Example 4: List all keys from Python
    io:format("~n4. List keys from Python:~n"),
    ok = py:exec(<<"
from erlang import state_keys

keys = state_keys()
assert 'hits' in keys
assert 'config' in keys
">>),
    Keys = py:state_keys(),
    io:format("   Keys: ~p~n", [Keys]),

    %% Example 5: Delete from Python
    io:format("~n5. Delete from Python:~n"),
    ok = py:exec(<<"
from erlang import state_delete

state_delete('computation_result')
">>),
    case py:state_fetch(<<"computation_result">>) of
        {error, not_found} -> io:format("   Key deleted successfully~n");
        {ok, _} -> io:format("   ERROR: Key still exists~n")
    end,

    %% Cleanup
    py:state_clear(),

    io:format("~n=== Done ===~n~n"),
    ok = application:stop(erlang_python).
