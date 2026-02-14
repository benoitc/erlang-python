#!/usr/bin/env escript
%%% @doc Semantic search example using sentence-transformers.
%%%
%%% This example demonstrates:
%%%   - Loading documents and computing embeddings
%%%   - Building a searchable index
%%%   - Finding semantically similar documents
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%   2. Create a venv and install sentence-transformers:
%%%        python3.14 -m venv /tmp/ai-venv
%%%        /tmp/ai-venv/bin/pip install sentence-transformers numpy
%%%
%%% Run from project root:
%%%   escript examples/semantic_search.erl
%%%
%%% Or with custom venv path:
%%%   escript examples/semantic_search.erl /path/to/your/venv

-mode(compile).

main(Args) ->
    setup_paths(),
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== Semantic Search Example ===~n~n"),

    %% Activate venv
    VenvPath = case Args of
        [Path | _] -> Path;
        [] -> "/tmp/ai-venv"
    end,

    case activate_venv(VenvPath) of
        ok -> ok;
        error -> halt(1)
    end,

    %% Initialize - add examples dir to Python path
    ExamplesDir = examples_dir(),
    ok = add_to_python_path(ExamplesDir),

    %% Load embedding model
    io:format("Loading embedding model...~n"),
    {ok, Info} = py:call(ai_helpers, model_info, []),
    io:format("Model loaded: ~p~n~n", [Info]),

    %% Sample documents
    Documents = [
        {<<"doc1">>, <<"Erlang is a programming language used to build massively scalable soft real-time systems with requirements on high availability.">>},
        {<<"doc2">>, <<"Python is widely used for machine learning, data science, and artificial intelligence applications.">>},
        {<<"doc3">>, <<"The BEAM virtual machine executes Erlang and Elixir code with lightweight processes and message passing.">>},
        {<<"doc4">>, <<"Neural networks are computing systems inspired by biological neural networks that constitute animal brains.">>},
        {<<"doc5">>, <<"Distributed systems are systems whose components are located on different networked computers.">>},
        {<<"doc6">>, <<"Natural language processing enables computers to understand, interpret, and generate human language.">>},
        {<<"doc7">>, <<"Fault tolerance is the property that enables a system to continue operating properly in the event of failure.">>},
        {<<"doc8">>, <<"Deep learning is part of machine learning based on artificial neural networks with representation learning.">>}
    ],

    %% Build index
    io:format("Building search index for ~p documents...~n", [length(Documents)]),
    Index = build_index(Documents),
    io:format("Index built!~n"),

    %% Run searches
    Queries = [
        <<"concurrent programming and message passing">>,
        <<"artificial intelligence and learning">>,
        <<"system reliability and failures">>,
        <<"understanding human text">>
    ],

    lists:foreach(fun(Query) ->
        io:format("~n--- Query: ~s ---~n", [Query]),
        Results = search(Query, Index, 3),
        lists:foreach(fun({Score, Id, Text}) ->
            ShortText = truncate(Text, 60),
            io:format("  [~.3f] ~s: ~s~n", [Score, Id, ShortText])
        end, Results)
    end, Queries),

    io:format("~n=== Done ===~n~n"),
    cleanup().

setup_paths() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir).

examples_dir() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    list_to_binary(filename:join(ProjectRoot, "examples")).

activate_venv(VenvPath) ->
    io:format("Activating venv: ~s~n", [VenvPath]),
    case py:activate_venv(list_to_binary(VenvPath)) of
        ok ->
            io:format("Venv activated~n"),
            ok;
        {error, VenvError} ->
            io:format("Error: ~p~n", [VenvError]),
            io:format("~nSetup instructions:~n"),
            io:format("  python3.14 -m venv ~s~n", [VenvPath]),
            io:format("  ~s/bin/pip install sentence-transformers numpy~n~n", [VenvPath]),
            error
    end.

add_to_python_path(Dir) ->
    {ok, _} = py:eval(<<"(__import__('sys').path.insert(0, path) if path not in __import__('sys').path else None, True)[1]">>, #{path => Dir}),
    ok.

build_index(Documents) ->
    {Ids, Texts} = lists:unzip(Documents),
    {ok, Embeddings} = py:call(ai_helpers, embed_texts, [Texts]),
    lists:zip3(Ids, Texts, Embeddings).

search(Query, Index, TopK) ->
    {ok, QueryEmb} = py:call(ai_helpers, embed_single, [Query]),
    Scored = [{cosine_similarity(QueryEmb, Emb), Id, Text}
              || {Id, Text, Emb} <- Index],
    Sorted = lists:reverse(lists:sort(Scored)),
    lists:sublist(Sorted, TopK).

cosine_similarity(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([X * X || X <- Vec1])),
    Norm2 = math:sqrt(lists:sum([X * X || X <- Vec2])),
    Dot / (Norm1 * Norm2).

truncate(Text, MaxLen) when byte_size(Text) > MaxLen ->
    <<Short:MaxLen/binary, _/binary>> = Text,
    <<Short/binary, "...">>;
truncate(Text, _) ->
    Text.

cleanup() ->
    ok = py:deactivate_venv(),
    ok = application:stop(erlang_python).
