.PHONY: all compile test lint-docs clean

all: compile

compile:
	rebar3 compile

test:
	rebar3 ct --readable=compact

# Validate fenced code blocks in README.md and docs/*.md.
# Erlang `py:Fn(...)` calls must reference a real export at the right
# arity; Python blocks must parse (IndentationError tolerated for
# tutorial fragments). Mark a block to skip with `<!-- skip-lint -->`
# on the line immediately above the opening fence.
lint-docs: compile
	escript scripts/lint_doc_snippets.escript

clean:
	rebar3 clean
