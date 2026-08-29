#!/bin/sh
# Every source file must be listed in docs/code-map.md, every Erlang module
# must have a moduledoc, and every Erlang module must have a row in the
# "Modules" table of test/coverage_audit.md. Exit code is the number of
# failures. Run: sh scripts/check_code_map.sh
cd "$(dirname "$0")/.." || exit 2
fail=0
note() { echo "check_code_map: $1"; fail=$((fail + 1)); }

for f in src/*.erl; do
    m=$(basename "$f" .erl)
    grep -q "\`$m\`" docs/code-map.md || note "$m is not in docs/code-map.md"
    grep -q '^%%%\{0,1\} @doc' "$f" || note "$f has no @doc moduledoc"
    grep -q "^| \`$m\` |" test/coverage_audit.md || note "$m has no row in the Modules table of test/coverage_audit.md"
done
for f in c_src/*.c c_src/*.h priv/_erlang_impl/*.py priv/*.py; do
    b=$(basename "$f")
    grep -q "$b" docs/code-map.md || note "$b is not in docs/code-map.md"
done
[ "$fail" -eq 0 ] && echo "check_code_map: clean"
exit $fail
