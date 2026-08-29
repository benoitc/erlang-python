# Decision records

Why the code is the way it is, one decision per file, in the order they
were taken. Read the record before changing what it decided; if the
reasons no longer hold, write a new record that supersedes it rather than
editing the old one. Each record has the same four parts: the situation,
what was decided, what it costs, and where the code is.

| # | Decision | Since |
|---|---|---|
| [0001](0001-one-thread-per-context.md) | One pthread per context, NIFs only enqueue | 3.0.0 |
| [0002](0002-callback-delivery-paths.md) | Three callback delivery paths, chosen by the calling thread | 3.0.0 |
| [0003](0003-callback-results-as-etf.md) | Callback results cross as external term format | 4.0.0 |
| [0004](0004-isolated-mode-child-process.md) | Isolation is a child OS process over a Unix socket | 5.0.0 |
| [0005](0005-py-isolated-gen-statem.md) | The isolated context process is a gen_statem | 5.0.0 |
| [0006](0006-shared-memory-over-iommap.md) | Bulk data through iommap regions, handles as plain tuples | 5.0.0 |
| [0007](0007-remove-legacy-execution-paths.md) | One execution path per mode; the legacy API is removed | 5.0.0 |
| [0008](0008-pipe-io-rules.md) | Pipe I/O is non-blocking, deadlined and waited with poll | 3.1.0, 5.0.0 |
