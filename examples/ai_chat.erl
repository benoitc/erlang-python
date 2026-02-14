#!/usr/bin/env escript
%%% @doc AI Chat example - interactive conversation with an LLM.
%%%
%%% This example demonstrates:
%%%   - Connecting to local Ollama or OpenAI API
%%%   - Maintaining conversation history
%%%   - Interactive chat loop
%%%
%%% Prerequisites:
%%%   1. Build the project: rebar3 compile
%%%   2. Create a venv:
%%%        python3.14 -m venv /tmp/ai-venv
%%%        /tmp/ai-venv/bin/pip install requests
%%%
%%% For Ollama (recommended, free, local):
%%%   - Install: https://ollama.ai
%%%   - Run: ollama pull llama3.2
%%%
%%% For OpenAI:
%%%   - Set OPENAI_API_KEY environment variable
%%%   - pip install openai
%%%
%%% Run from project root:
%%%   escript examples/ai_chat.erl

-mode(compile).

main(Args) ->
    setup_paths(),
    {ok, _} = application:ensure_all_started(erlang_python),

    io:format("~n=== AI Chat Example ===~n~n"),

    VenvPath = case Args of
        [Path | _] -> Path;
        [] -> "/tmp/ai-venv"
    end,

    case activate_venv(VenvPath) of
        ok -> ok;
        error -> halt(1)
    end,

    %% Initialize chat
    io:format("Initializing chat system...~n"),
    ok = init_ai_helpers(),
    {ok, LLMType} = get_llm_type(),
    case LLMType of
        <<"none">> ->
            io:format("~nNo LLM available!~n~n"),
            io:format("Setup options:~n"),
            io:format("  1. Install Ollama: https://ollama.ai~n"),
            io:format("     Then run: ollama pull llama3.2~n~n"),
            io:format("  2. Set OPENAI_API_KEY environment variable~n"),
            io:format("     And: pip install openai~n~n"),
            cleanup(),
            halt(1);
        _ ->
            io:format("Using LLM: ~s~n~n", [LLMType]),
            io:format("Type your messages (or 'quit' to exit, 'clear' to reset)~n"),
            io:format("---------------------------------------------------~n~n"),
            chat_loop([])
    end.

setup_paths() ->
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "erlang_python", "ebin"]),
    true = code:add_pathz(EbinDir).

activate_venv(VenvPath) ->
    io:format("Activating venv: ~s~n", [VenvPath]),
    case py:activate_venv(list_to_binary(VenvPath)) of
        ok ->
            ok;
        {error, VenvError} ->
            io:format("Error: ~p~n", [VenvError]),
            io:format("~nSetup: python3.14 -m venv ~s && ~s/bin/pip install requests~n~n",
                      [VenvPath, VenvPath]),
            error
    end.

init_ai_helpers() ->
    %% Add examples directory to Python path
    ScriptDir = filename:dirname(escript:script_name()),
    ProjectRoot = filename:dirname(ScriptDir),
    ExamplesDir = list_to_binary(filename:join(ProjectRoot, "examples")),
    {ok, _} = py:eval(<<"(__import__('sys').path.insert(0, path) if path not in __import__('sys').path else None, True)[1]">>, #{path => ExamplesDir}),
    ok.

get_llm_type() ->
    py:call(ai_helpers, get_llm_type, []).

chat_loop(History) ->
    case io:get_line("You: ") of
        eof ->
            io:format("~nGoodbye!~n"),
            cleanup();
        {error, _} ->
            io:format("~nGoodbye!~n"),
            cleanup();
        Line ->
            Input = string:trim(Line),
            case string:lowercase(Input) of
                "quit" ->
                    io:format("~nGoodbye!~n"),
                    cleanup();
                "clear" ->
                    io:format("~n[Conversation cleared]~n~n"),
                    chat_loop([]);
                "" ->
                    chat_loop(History);
                _ ->
                    %% Add user message to history
                    UserMsg = #{role => <<"user">>, content => list_to_binary(Input)},
                    NewHistory = History ++ [UserMsg],

                    %% Get response
                    io:format("~nAssistant: "),
                    {ok, Response} = py:call(ai_helpers, chat, [NewHistory]),
                    io:format("~s~n~n", [Response]),

                    %% Add assistant message to history
                    AssistantMsg = #{role => <<"assistant">>, content => Response},
                    chat_loop(NewHistory ++ [AssistantMsg])
            end
    end.

cleanup() ->
    ok = py:deactivate_venv(),
    ok = application:stop(erlang_python).
