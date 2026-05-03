#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable
%%
%% lint_doc_snippets — verifies fenced code blocks in README.md and
%% docs/*.md.
%%
%%   * ```erlang blocks: every `py:Fn(Args)` call must reference a
%%     function exported from py.erl at the correct arity.
%%   * ```python blocks: must parse as Python (ast.parse). Anything
%%     stricter (undefined names, wrong types) is out of scope; we are
%%     after typos and removed-API references, not full type checking.
%%   * Skip with `<!-- skip-lint -->` on the line immediately above
%%     the opening fence. Use sparingly and explain why in the
%%     surrounding prose.
%%
%% Exit code is the number of failures (0 = clean).
%%
%% Run: rebar3 compile && escript scripts/lint_doc_snippets.escript

main(Args) ->
    Root = filename:dirname(filename:dirname(escript:script_name())),
    EbinDir = filename:join([Root, "_build", "default", "lib",
                             "erlang_python", "ebin"]),
    case filelib:is_dir(EbinDir) of
        true ->
            true = code:add_path(EbinDir);
        false ->
            io:format(standard_error,
                      "ebin not found at ~s; run `rebar3 compile` first~n",
                      [EbinDir]),
            halt(2)
    end,
    PyExports = load_py_exports(),
    Files = case Args of
        [] -> default_files(Root);
        _  -> Args
    end,
    Failures = lists:foldl(
        fun(File, Acc) -> Acc + lint_file(File, PyExports) end,
        0, Files),
    case Failures of
        0 ->
            io:format("lint-docs: clean (~p file(s))~n", [length(Files)]),
            halt(0);
        N ->
            io:format(standard_error,
                      "lint-docs: ~p failure(s)~n", [N]),
            halt(N)
    end.

default_files(Root) ->
    Readme = filename:join(Root, "README.md"),
    Docs   = filelib:wildcard(filename:join([Root, "docs", "*.md"])),
    [Readme | Docs].

load_py_exports() ->
    case code:ensure_loaded(py) of
        {module, py} ->
            sets:from_list(py:module_info(exports));
        Other ->
            io:format(standard_error,
                      "could not load py.beam: ~p~n", [Other]),
            halt(2)
    end.

%%% ----------------------------------------------------------------
%%% per-file walk
%%% ----------------------------------------------------------------

lint_file(File, PyExports) ->
    case file:read_file(File) of
        {ok, Bin} ->
            Lines = binary:split(Bin, <<"\n">>, [global]),
            Numbered = lists:zip(lists:seq(1, length(Lines)), Lines),
            Blocks = collect_blocks(Numbered, <<>>, []),
            lists:foldl(
                fun(Block, Acc) -> Acc + lint_block(File, Block, PyExports) end,
                0, Blocks);
        {error, Reason} ->
            io:format(standard_error, "~s: read failed (~p)~n",
                      [File, Reason]),
            1
    end.

%% Walk numbered lines and emit {Lang, StartLine, BodyBin} for each
%% ```LANG ... ``` block. PrevLine carries the previous line so we can
%% detect a `<!-- skip-lint -->` marker on the line immediately before
%% an opening fence.
collect_blocks([], _Prev, Acc) ->
    lists:reverse(Acc);
collect_blocks([{_, Line} | Rest], Prev, Acc) ->
    case classify(Line) of
        {open, Lang} ->
            Skip = is_skip_marker(Prev),
            {Body, RestAfter, StartLine} =
                consume_block(Rest, [], Skip, Lang),
            NewAcc = case Skip of
                true  -> Acc;
                false -> [{Lang, StartLine, Body} | Acc]
            end,
            collect_blocks(RestAfter, Line, NewAcc);
        _ ->
            collect_blocks(Rest, Line, Acc)
    end.

is_skip_marker(Line) ->
    Trim = string:trim(Line),
    Trim =:= <<"<!-- skip-lint -->">>.

classify(Line) ->
    Trim = string:trim(Line),
    case Trim of
        <<"```erlang">> -> {open, erlang};
        <<"```python">> -> {open, python};
        <<"```py">>     -> {open, python};
        <<"```">>       -> close;
        _               -> other
    end.

%% Consume lines until the closing ``` fence. Returns {BodyBin, RestLines, FirstLine}.
consume_block([], BodyAcc, _Skip, _Lang) ->
    {iolist_to_binary(lists:reverse(BodyAcc)), [], 0};
consume_block([{LineNo, Line} | Rest], BodyAcc, _Skip, Lang) ->
    case classify(Line) of
        close ->
            {iolist_to_binary(lists:reverse(BodyAcc)), Rest, LineNo};
        _ ->
            consume_block(Rest, [<<Line/binary, "\n">> | BodyAcc],
                          _Skip, Lang)
    end.


%%% ----------------------------------------------------------------
%%% per-block lint
%%% ----------------------------------------------------------------

lint_block(File, {erlang, _Line, Body}, PyExports) ->
    lint_erlang_block(File, Body, PyExports);
lint_block(File, {python, _Line, Body}, _) ->
    lint_python_block(File, Body);
lint_block(_, _, _) ->
    0.

%% Erlang: scan tokens, look for `py:Fn(...)` patterns and check exports.
lint_erlang_block(File, Body, PyExports) ->
    case erl_scan:string(unicode:characters_to_list(Body)) of
        {ok, Tokens, _} ->
            check_py_calls(File, Tokens, PyExports);
        {error, _Info, _Loc} ->
            %% Erlang shell-style snippets (`1> ...`) and tutorial
            %% fragments don't tokenise. Don't fail on those — the
            %% goal is to catch wrong API names, not enforce
            %% standalone parseability.
            0
    end.

%% Walk the token list looking for the pattern:
%%     atom(py)  ':'  atom(Fn)  '('  ...  ')'
%% The counter consumes tokens up to and including the matching `)`.
check_py_calls(_File, [], _Exports) -> 0;
check_py_calls(File, [{atom, _, py}, {':', _}, {atom, Loc, Fn},
                      {'(', _} | Rest], Exports) ->
    {Arity, After} = count_top_level_args(Rest, 1, 0, 0),
    Bad = case sets:is_element({Fn, Arity}, Exports) of
        true  -> 0;
        false ->
            io:format(standard_error,
                      "~s:~p: undefined or wrong-arity call py:~s/~p~n",
                      [File, line_of(Loc), atom_to_list(Fn), Arity]),
            1
    end,
    Bad + check_py_calls(File, After, Exports);
check_py_calls(File, [_ | Rest], Exports) ->
    check_py_calls(File, Rest, Exports).

line_of({Line, _Col}) -> Line;
line_of(Line) when is_integer(Line) -> Line;
line_of(_) -> 0.

%% Count top-level arguments in a function call. Tracks nesting of
%% `(`, `[`, `{` so commas inside nested expressions are ignored.
%% Returns {Arity, TokensAfterClosingParen} — the caller advances past
%% the call before resuming the scan, so a `py:foo(...)` nested inside
%% another `py:bar(...)` is not double-counted.
count_top_level_args([{')', _} | Rest], 1, 0, _Args) ->
    %% Empty arg list `f()`.
    {0, Rest};
count_top_level_args([{')', _} | Rest], 1, _Args, ArgsSoFar) ->
    %% Closing the outer `(` — return arg count.
    {ArgsSoFar + 1, Rest};
count_top_level_args([], _Depth, ArgsSeen, ArgsSoFar) ->
    %% Truncated snippet; best-effort count.
    {ArgsSoFar + (case ArgsSeen of 0 -> 0; _ -> 1 end), []};
count_top_level_args([{Tok, _} | Rest], Depth, _Args, ArgsSoFar)
        when Tok =:= '('; Tok =:= '['; Tok =:= '{'; Tok =:= '<<' ->
    count_top_level_args(Rest, Depth + 1, 1, ArgsSoFar);
count_top_level_args([{Tok, _} | Rest], Depth, _Args, ArgsSoFar)
        when Tok =:= ')'; Tok =:= ']'; Tok =:= '}'; Tok =:= '>>' ->
    count_top_level_args(Rest, Depth - 1, 1, ArgsSoFar);
%% `fun`, `case`, `if`, `try`, `receive`, `begin` open an `end`-terminated
%% block. Treat them as depth brackets so commas inside fun bodies
%% (which are part of one outer arg) are not counted.
count_top_level_args([{Kw, _} | Rest], Depth, _Args, ArgsSoFar)
        when Kw =:= 'fun'; Kw =:= 'case'; Kw =:= 'if'; Kw =:= 'try';
             Kw =:= 'receive'; Kw =:= 'begin' ->
    count_top_level_args(Rest, Depth + 1, 1, ArgsSoFar);
count_top_level_args([{'end', _} | Rest], Depth, _Args, ArgsSoFar) ->
    count_top_level_args(Rest, Depth - 1, 1, ArgsSoFar);
count_top_level_args([{',', _} | Rest], 1, _ArgsSeen, ArgsSoFar) ->
    count_top_level_args(Rest, 1, 0, ArgsSoFar + 1);
count_top_level_args([_Tok | Rest], Depth, _Args, ArgsSoFar) ->
    count_top_level_args(Rest, Depth, 1, ArgsSoFar).

%% Python: write the snippet to a temp file and run ast.parse on it.
%% IndentationError / TabError are tolerated because tutorial-style
%% fragments often omit outer scope (e.g. show only the body of an
%% if-block, or a single line of a larger function). SyntaxError still
%% fails the lint.
lint_python_block(File, Body) ->
    TmpDir = case os:getenv("TMPDIR") of
        false -> "/tmp";
        Dir   -> Dir
    end,
    Tmp = filename:join(TmpDir, "lint_snip_"
                        ++ integer_to_list(erlang:unique_integer([positive]))
                        ++ ".py"),
    ok = file:write_file(Tmp, Body),
    Script =
        "import ast,sys\n"
        "src=open(sys.argv[1]).read()\n"
        "try:\n"
        "    ast.parse(src)\n"
        "except (IndentationError, TabError):\n"
        "    sys.exit(0)\n"
        "except SyntaxError as e:\n"
        "    print(f'SyntaxError: {e}')\n"
        "    sys.exit(1)\n",
    Cmd = "python3 -c " ++ shell_quote(Script) ++ " " ++ Tmp
          ++ " 2>&1; echo __exit_$?__",
    Out = os:cmd(Cmd),
    file:delete(Tmp),
    case re:run(Out, <<"__exit_0__\\s*$">>, [{capture, none}]) of
        match -> 0;
        nomatch ->
            Trimmed = re:replace(Out, <<"__exit_\\d+__\\s*$">>, <<>>,
                                 [{return, list}]),
            io:format(standard_error,
                      "~s: python snippet failed:~n~s~n",
                      [File, Trimmed]),
            1
    end.

%% Single-quote a string for safe inclusion in /bin/sh.
shell_quote(S) ->
    Escaped = re:replace(S, "'", "'\\\\''", [global, {return, list}]),
    "'" ++ Escaped ++ "'".
