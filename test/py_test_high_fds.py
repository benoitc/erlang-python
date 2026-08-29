"""Thread callbacks with pipe fds above FD_SETSIZE.

Opens enough files to push every fd the runtime creates afterwards above
1024, then has several new threads call Erlang at once so fresh thread
workers (and their pipes) are created in that range. select() cannot
watch such fds; poll() can.
"""
import concurrent.futures
import os
import resource

_kept = []


def prepare(target=1200):
    """Raise the fd soft limit and fill descriptors up to `target`.

    Returns the highest fd opened, or -1 if the hard limit is too low.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    want = target + 256
    if hard != resource.RLIM_INFINITY and hard < want:
        return -1
    if soft < want:
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    last = -1
    while last < target:
        fd = os.open(os.devnull, os.O_RDONLY)
        _kept.append(fd)
        last = fd
    return last


def call_from_threads(n):
    import erlang
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
        futures = [ex.submit(erlang.call, 'high_fd_add', i, 1) for i in range(n)]
        results = []
        for f in futures:
            try:
                results.append(f.result())
            except Exception as exc:
                results.append('error: %s' % exc)
    return results


def cleanup():
    while _kept:
        os.close(_kept.pop())
    return 'ok'
