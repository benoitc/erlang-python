# Erlang Concurrency helpers for Python
# This module demonstrates calling Erlang functions from Python
# and leveraging Erlang's native concurrency model.
#
# The 'erlang' module is automatically available when running Python
# code via py:eval/py:call. It provides erlang.call() for invoking
# registered Erlang functions.
#
# KEY INSIGHT: Parallelism should happen on the Erlang side!
# Python calls Erlang with a batch of work, and Erlang spawns
# lightweight processes to handle each item concurrently.

import time


def call_erlang(name, *args):
    """Synchronously call a registered Erlang function.

    The 'erlang' module is injected by erlang_python into the
    Python environment. It provides erlang.call(name, *args).
    """
    import erlang
    return erlang.call(name, *args)


def parallel_map(func_name, items):
    """Map an Erlang function over items using Erlang's parallel processes.

    This calls a special Erlang function that spawns one process per item,
    processes them concurrently, and returns all results.

    Args:
        func_name: Name of registered Erlang function to apply
        items: List of items to process

    Returns:
        List of results in the same order as items
    """
    # Call the parallel_map Erlang function which spawns processes
    return call_erlang('parallel_map', func_name, items)


def demo_sequential(func_name, items):
    """Run tasks sequentially for comparison.

    Returns (results, elapsed_time) tuple.
    """
    start = time.time()
    results = [call_erlang(func_name, item) for item in items]
    elapsed = time.time() - start
    return results, elapsed


def demo_parallel(func_name, items):
    """Run tasks in parallel using Erlang processes.

    Returns (results, elapsed_time) tuple.
    """
    start = time.time()
    results = parallel_map(func_name, items)
    elapsed = time.time() - start
    return results, elapsed


def benchmark_comparison(func_name, items):
    """Compare sequential vs parallel execution.

    Returns dict with timing and speedup info.
    """
    # Sequential
    seq_results, seq_time = demo_sequential(func_name, items)

    # Parallel
    par_results, par_time = demo_parallel(func_name, items)

    speedup = seq_time / max(par_time, 0.001)

    return {
        'sequential_time': seq_time,
        'parallel_time': par_time,
        'speedup': speedup,
        'items_count': len(items),
        'sequential_results': seq_results,
        'parallel_results': par_results
    }


# Batch operations that leverage Erlang's concurrency
def batch_compute(items):
    """Compute results for multiple items in parallel via Erlang."""
    return call_erlang('spawn_workers', items)


def batch_fetch(urls):
    """Fetch multiple URLs in parallel via Erlang processes."""
    return call_erlang('parallel_fetch', urls)


def process_pipeline(items, stages):
    """Process items through a pipeline of Erlang functions.

    Each stage processes all items in parallel before passing
    to the next stage.

    Args:
        items: Initial list of items
        stages: List of Erlang function names

    Returns:
        Final processed results
    """
    current = items
    for stage in stages:
        current = parallel_map(stage, current)
    return current
