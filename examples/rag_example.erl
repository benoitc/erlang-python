#!/usr/bin/env escript
%%% @doc RAG (Retrieval-Augmented Generation) example.
%%%
%%% This example demonstrates:
%%%   - Building a knowledge base with embeddings
%%%   - Retrieving relevant context for questions
%%%   - Generating answers using an LLM (simulated or real)
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%   2. Create a venv:
%%%        python3.14 -m venv /tmp/ai-venv
%%%        /tmp/ai-venv/bin/pip install sentence-transformers numpy requests
%%%
%%% For real LLM (optional):
%%%   - Install Ollama: https://ollama.ai
%%%   - Run: ollama pull llama3.2
%%%   - Or set OPENAI_API_KEY and pip install openai
%%%
%%% Run from project root:
%%%   escript examples/rag_example.erl

-mode(compile).

main(Args) ->
    setup_paths(),
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== RAG (Retrieval-Augmented Generation) Example ===~n~n"),

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

    %% Load model and check LLM
    io:format("Initializing RAG system...~n"),
    {ok, _Info} = py:call(ai_helpers, model_info, []),
    {ok, LLMType} = py:call(ai_helpers, get_llm_type, []),
    io:format("Using LLM: ~s~n", [LLMType]),
    io:format("RAG system ready!~n~n"),

    %% Build knowledge base
    KnowledgeBase = [
        <<"Erlang was created at Ericsson in 1986 by Joe Armstrong, Robert Virding, and Mike Williams.">>,
        <<"The BEAM virtual machine is the runtime for Erlang and Elixir, known for its lightweight processes.">>,
        <<"Erlang processes are extremely lightweight, allowing millions of concurrent processes on a single machine.">>,
        <<"OTP (Open Telecom Platform) provides libraries and design patterns for building robust Erlang applications.">>,
        <<"Pattern matching in Erlang allows elegant handling of different message types and data structures.">>,
        <<"Erlang's 'let it crash' philosophy simplifies error handling through supervision trees.">>,
        <<"Hot code reloading in Erlang allows updating running systems without downtime.">>,
        <<"Message passing is the only way processes communicate in Erlang, ensuring no shared state.">>,
        <<"Elixir is a modern language that runs on the BEAM VM and provides Ruby-like syntax.">>,
        <<"The actor model in Erlang isolates processes and provides fault tolerance through isolation.">>
    ],

    io:format("Building knowledge base with ~p documents...~n", [length(KnowledgeBase)]),
    Index = build_index(KnowledgeBase),
    io:format("Knowledge base ready!~n~n"),

    %% Questions to answer
    Questions = [
        <<"When was Erlang created and by whom?">>,
        <<"How do Erlang processes communicate?">>,
        <<"What is the philosophy for handling errors in Erlang?">>,
        <<"Can you update Erlang code while the system is running?">>
    ],

    lists:foreach(fun(Question) ->
        io:format("~n========================================~n"),
        io:format("Question: ~s~n", [Question]),
        io:format("========================================~n~n"),

        %% Retrieve relevant context
        Context = retrieve(Question, Index, 2),
        io:format("Retrieved context:~n"),
        lists:foreach(fun({Score, Text}) ->
            io:format("  [~.3f] ~s~n", [Score, Text])
        end, Context),

        %% Generate answer
        io:format("~nGenerating answer...~n"),
        Answer = generate_answer(Question, Context),
        io:format("~nAnswer: ~s~n", [Answer])
    end, Questions),

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
            io:format("  ~s/bin/pip install sentence-transformers numpy requests~n~n", [VenvPath]),
            error
    end.

add_to_python_path(Dir) ->
    {ok, _} = py:eval(<<"(__import__('sys').path.insert(0, path) if path not in __import__('sys').path else None, True)[1]">>, #{path => Dir}),
    ok.

build_index(Documents) ->
    {ok, Embeddings} = py:call(ai_helpers, embed_texts, [Documents]),
    lists:zip(Documents, Embeddings).

retrieve(Query, Index, TopK) ->
    {ok, QueryEmb} = py:call(ai_helpers, embed_single, [Query]),
    Scored = [{cosine_similarity(QueryEmb, Emb), Text}
              || {Text, Emb} <- Index],
    Sorted = lists:reverse(lists:sort(Scored)),
    lists:sublist(Sorted, TopK).

generate_answer(Question, Context) ->
    ContextText = iolist_to_binary(
        lists:join(<<"\n">>, [Text || {_, Text} <- Context])
    ),
    {ok, Answer} = py:call(ai_helpers, generate, [Question, ContextText]),
    Answer.

cosine_similarity(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([X * X || X <- Vec1])),
    Norm2 = math:sqrt(lists:sum([X * X || X <- Vec2])),
    Dot / (Norm1 * Norm2).

cleanup() ->
    ok = py:deactivate_venv(),
    ok = application:stop(erlang_python).
