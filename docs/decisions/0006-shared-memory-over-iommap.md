# 0006: Bulk data through iommap regions, handles as plain tuples

Since 5.0.0. Code: `src/py_shm.erl`, `src/py_buffer.erl` (shared variant),
`priv/_erlang_impl/_shm.py`, the tagged-tuple case in `c_src/py_convert.c`.

## Situation

An isolated call copies its arguments and result through the socket:
1.3 ms per MB, 300 ms for 64 MB. Request bodies, arrays and model inputs
need a path that does not copy, and it must work the same in a pool that
mixes embedded and isolated contexts.

## Decision

A region is a file mapped `MAP_SHARED` by the VM through iommap
(`region_binary/3` gives a refcounted binary with no copy) and by any
interpreter through `mmap`. Its handle is the plain term
`{'$py_shm', Id, Path, Size}`, so it travels inside any argument or
result with no special encoding and becomes a `SharedMemory` on arrival
in every mode (the C converter and the child both call `_shm.from_term`).
A shared `py_buffer` is a region used as a ring, with flow control
through registered callbacks (`_py_buffer_wait`, `_py_buffer_consumed`),
the mechanism channels already use, so no new control frames exist.
iommap is optional: `py_shm:new/1` returns `{error, iommap_not_available}`
without it.

Not chosen: passing the region's fd (iommap exposes none; a path in a
0700 directory is enough), a NIF of our own for mapping, and channels
over shared memory (small terms stay on the socket).

## Consequences

- Python-produced data is zero-copy both ways; Erlang-produced data
  costs one `pwrite` because a binary cannot be written in place.
- Sharing memory weakens isolation for that region only; read-only
  handles keep a callee from writing. Sealing and syscall filtering are
  separate work.
- In embedded contexts a shared buffer costs a callback round trip per
  blocking read where the native buffer costs a pointer; the guide says
  when to use which.
- The region file must never be truncated (`SIGBUS`); sizes are fixed at
  creation and verified with `fstat` before mapping.
