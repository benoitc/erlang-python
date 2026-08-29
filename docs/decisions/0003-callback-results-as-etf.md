# 0003: Callback results cross as external term format

Since 4.0.0. Code: `handle_blocking_callback/3` and friends in
`src/py_context.erl`, `parse_callback_response` in `c_src/py_callback.c`,
`priv/_erlang_impl/_etf.py` for the child.

## Situation

Results of an Erlang callback used to reach Python as the Python repr of
the term, parsed with `ast.literal_eval`. Binaries with backslashes,
quotes or newlines produced unparseable literals and were handed over as
raw text, `[]` arrived as `''`, floats lost precision, and pids and
references went through a base64 marker that had to be decoded with an
unsafe `binary_to_term`.

## Decision

Results are encoded with `term_to_binary` and decoded by the same
`term_to_py` converter that call arguments use, with
`ERL_NIF_BIN2TERM_SAFE`. Pids and references cross as native objects.
The one visible change is accepted as breaking: an Erlang string
(`"abc"`) reaches Python as `[97, 98, 99]`, exactly as it does for
arguments; return a binary for a `str`.

## Consequences

- One type mapping in both directions, documented once
  (`docs/type-conversion.md`, `c_src/py_convert.c`).
- The `__etf__:` marker is data, never re-interpreted;
  `test_etf_decode_safe` guards that no atoms are minted.
- The child needs a Python ETF codec (`_etf.py`) that produces the same
  terms as the C converter; keeping them in step is a maintenance duty.
