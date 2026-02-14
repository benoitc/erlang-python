#!/usr/bin/env escript
%%% @doc Example of using sentence-transformers for text embeddings.
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%   2. Create a venv and install sentence-transformers:
%%%        python -m venv /tmp/embedder-venv
%%%        /tmp/embedder-venv/bin/pip install sentence-transformers
%%%
%%% Run from project root:
%%%   escript examples/embedder_example.erl
%%%
%%% Or with custom venv path:
%%%   escript examples/embedder_example.erl /path/to/your/venv

-mode(compile).

main(Args) ->
    %% Setup code paths
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir),

    %% Start the application
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Sentence-Transformers Embedder Example ===~n~n"),

    %% Activate venv
    VenvPath = case Args of
        [Path | _] -> Path;
        [] -> "/tmp/embedder-venv"
    end,

    io:format("Activating venv: ~s~n", [VenvPath]),
    case py:activate_venv(VenvPath) of
        ok ->
            io:format("Venv activated~n~n");
        {error, VenvError} ->
            io:format("Error: ~p~n", [VenvError]),
            io:format("Run: python -m venv ~s && ~s/bin/pip install sentence-transformers~n", [VenvPath, VenvPath]),
            halt(1)
    end,

    %% Add examples directory to Python path and import module with reload
    ExamplesDir = filename:join(ProjectRoot, "examples"),
    PathSetup = list_to_binary(io_lib:format(
        "(lambda: (__import__('sys').path.insert(0, '~s') if '~s' not in __import__('sys').path else None, __import__('importlib').reload(__import__('embedding_helper'))))()[1]",
        [ExamplesDir, ExamplesDir])),

    %% Get model info (this loads the model)
    io:format("Loading model...~n"),
    {ok, Info} = py:eval(<<PathSetup/binary, ".model_info()">>),
    io:format("Model: ~p~n~n", [Info]),

    %% Embed some texts
    Texts = [
        <<"Erlang is great for concurrent systems">>,
        <<"Python is popular for machine learning">>,
        <<"Elixir runs on the BEAM virtual machine">>,
        <<"JavaScript is used for web development">>
    ],

    {ok, Embeddings} = py:eval(<<PathSetup/binary, ".embed(texts)">>, #{texts => Texts}),

    io:format("=== Embeddings ===~n"),
    lists:foreach(
        fun({Text, Emb}) ->
            io:format("  ~s~n    -> [~.4f, ~.4f, ...] (dim=~p)~n",
                      [Text, hd(Emb), lists:nth(2, Emb), length(Emb)])
        end,
        lists:zip(Texts, Embeddings)
    ),

    %% Compute similarities
    io:format("~n=== Semantic Similarity ===~n"),
    Query = <<"concurrent programming language">>,
    {ok, QueryEmb} = py:eval(<<PathSetup/binary, ".embed(q)">>, #{q => Query}),

    io:format("Query: ~s~n~n", [Query]),

    Similarities = [{cosine_similarity(QueryEmb, Emb), Text}
                    || {Text, Emb} <- lists:zip(Texts, Embeddings)],
    Sorted = lists:reverse(lists:keysort(1, Similarities)),

    lists:foreach(
        fun({Sim, Text}) ->
            io:format("  ~.4f: ~s~n", [Sim, Text])
        end,
        Sorted
    ),

    io:format("~n=== Done ===~n~n"),
    ok = py:deactivate_venv(),
    ok = application:stop(erlang_python).

cosine_similarity(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([X * X || X <- Vec1])),
    Norm2 = math:sqrt(lists:sum([X * X || X <- Vec2])),
    Dot / (Norm1 * Norm2).
