.PHONY: all compile test lint-docs check-code-map clean

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

# Every source file in docs/code-map.md, every module with a moduledoc and
# a row in the Modules table of test/coverage_audit.md.
check-code-map:
	sh scripts/check_code_map.sh

clean:
	rebar3 clean
