# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Async-aware test runner that properly integrates with ErlangEventLoop.

Uses erlang.run() to execute tests, ensuring timer callbacks fire
correctly. This solves the problem where unittest's synchronous model blocks
the event loop and prevents Erlang timer integration from working.

The unified 'erlang' module now provides both callback support (call, async_call)
and event loop API (run, new_event_loop, EventLoopPolicy).

Usage from Erlang:
    {ok, Results} = py:call(Ctx, 'tests.async_test_runner', run_tests,
                            [<<"tests.test_base">>, <<"TestErlang*">>]).

Timer Flow with erlang.run():
    erlang.run(run_all())
        │
        └─→ ErlangEventLoop.run_until_complete()
                │
                └─→ _run_once() loop
                    ├─ Processes ready callbacks
                    ├─ Calculates timeout from timer heap
                    └─ Calls _pel._run_once_native(timeout)
                        │
                        └─→ Polls Erlang scheduler (GIL released!)
                            │
                            └─→ Timer fires via erlang:send_after
                                │
                                └─→ Callback dispatched back to Python
"""

import asyncio
import fnmatch
import io
import sys
import traceback
import unittest
from typing import Dict, Any, List

# Import erlang module for proper event loop integration
# The unified erlang module now provides both callbacks and event loop API.
try:
    import erlang
    _has_erlang = hasattr(erlang, 'run')
except ImportError:
    _has_erlang = False


async def run_test_method(test_case, method_name: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Run a single test method with timeout support using Erlang timers.

    Args:
        test_case: The test case class
        method_name: Name of the test method to run
        timeout: Per-test timeout in seconds

    Returns:
        Dict with test result including name, status, and error if any
    """
    test = test_case(method_name)
    result = {
        'name': f"{test_case.__name__}.{method_name}",
        'status': 'ok',
        'error': None
    }

    try:
        # Setup
        if hasattr(test, 'setUp'):
            setup_method = test.setUp
            if asyncio.iscoroutinefunction(setup_method):
                await setup_method()
            else:
                setup_method()

        # Run test with timeout using asyncio (backed by Erlang timers)
        method = getattr(test, method_name)
        if asyncio.iscoroutinefunction(method):
            await asyncio.wait_for(method(), timeout=timeout)
        else:
            # For sync tests, wrap in executor to avoid blocking the event loop
            loop = asyncio.get_running_loop()
            await asyncio.wait_for(
                loop.run_in_executor(None, method),
                timeout=timeout
            )

    except asyncio.TimeoutError:
        result['status'] = 'timeout'
        result['error'] = f"Test timed out after {timeout}s"
    except unittest.SkipTest as e:
        result['status'] = 'skipped'
        result['error'] = str(e)
    except AssertionError as e:
        result['status'] = 'failure'
        result['error'] = traceback.format_exc()
    except Exception as e:
        result['status'] = 'error'
        result['error'] = traceback.format_exc()
    finally:
        try:
            if hasattr(test, 'tearDown'):
                teardown_method = test.tearDown
                if asyncio.iscoroutinefunction(teardown_method):
                    await teardown_method()
                else:
                    teardown_method()
        except Exception:
            # Don't let teardown failures mask test failures
            pass

    return result


async def run_test_class(test_class, timeout: float = 30.0) -> List[Dict[str, Any]]:
    """Run all test methods in a test class.

    Args:
        test_class: The test case class to run
        timeout: Per-test timeout in seconds

    Returns:
        List of test result dicts
    """
    results = []
    loader = unittest.TestLoader()

    for method_name in loader.getTestCaseNames(test_class):
        result = await run_test_method(test_class, method_name, timeout)
        results.append(result)

    return results


def run_tests(module_name: str, pattern: str, timeout: float = 30.0) -> Dict[str, Any]:
    """
    Run tests matching pattern using ErlangEventLoop.

    This function uses erlang.run() to properly execute async code
    with Erlang's timer integration. This is the key difference from
    the sync ct_runner - timers actually fire because we're using
    the Erlang-backed event loop.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')
        pattern: fnmatch pattern for test class names (e.g., 'TestErlang*')
        timeout: Timeout in seconds for each individual test (default 30s)

    Returns:
        Dictionary with keys:
            - tests_run: Number of tests executed
            - failures: Number of test failures
            - errors: Number of test errors
            - skipped: Number of skipped tests
            - success: Boolean indicating all tests passed
            - output: Formatted test output (string)
            - failure_details: List of failure/error details
    """
    # Handle binary strings from Erlang
    if isinstance(module_name, bytes):
        module_name = module_name.decode('utf-8')
    if isinstance(pattern, bytes):
        pattern = pattern.decode('utf-8')

    async def run_all():
        """Async inner function to run all matching tests."""
        module = __import__(module_name, fromlist=[''])
        all_results = []

        # Find all test classes matching pattern
        for name in dir(module):
            if fnmatch.fnmatch(name, pattern):
                obj = getattr(module, name)
                if isinstance(obj, type) and issubclass(obj, unittest.TestCase):
                    if obj is not unittest.TestCase:
                        results = await run_test_class(obj, timeout)
                        all_results.extend(results)

        return all_results

    try:
        # Use erlang.run() - this properly integrates with Erlang timers!
        # This is the key difference from ct_runner.py which uses ThreadPoolExecutor
        if _has_erlang:
            results = erlang.run(run_all())
        else:
            # Fallback for testing outside Erlang VM
            results = asyncio.run(run_all())

        # Aggregate results
        tests_run = len(results)
        failures = sum(1 for r in results if r['status'] == 'failure')
        errors = sum(1 for r in results if r['status'] in ('error', 'timeout'))
        skipped = sum(1 for r in results if r['status'] == 'skipped')

        # Build failure details for CT reporting
        failure_details = []
        for r in results:
            if r['status'] in ('failure', 'error', 'timeout'):
                failure_details.append({
                    'test': r['name'],
                    'traceback': r['error'] or ''
                })

        return {
            'tests_run': tests_run,
            'failures': failures,
            'errors': errors,
            'skipped': skipped,
            'success': failures == 0 and errors == 0,
            'results': results,
            'output': _format_results(results),
            'failure_details': failure_details
        }
    except Exception as e:
        return {
            'tests_run': 0,
            'failures': 0,
            'errors': 1,
            'skipped': 0,
            'success': False,
            'results': [],
            'output': traceback.format_exc(),
            'failure_details': [{'test': 'import', 'traceback': str(e)}]
        }


def _format_results(results: List[Dict]) -> str:
    """Format results as text output for CT logs.

    Args:
        results: List of test result dicts

    Returns:
        Formatted string output
    """
    lines = []
    status_map = {
        'ok': 'ok',
        'failure': 'FAIL',
        'error': 'ERROR',
        'timeout': 'TIMEOUT',
        'skipped': 'skipped'
    }

    for r in results:
        status = status_map.get(r['status'], r['status'])
        lines.append(f"{r['name']} ... {status}")
        if r['error'] and r['status'] != 'ok':
            # Indent error output
            error_lines = r['error'].split('\n')
            for line in error_lines:
                lines.append(f"  {line}")

    # Summary line
    lines.append("")
    lines.append("-" * 70)
    total = len(results)
    lines.append(f"Ran {total} test{'s' if total != 1 else ''}")

    return '\n'.join(lines)


def run_erlang_tests(module_name: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Run only Erlang event loop tests from a module.

    Convenience function that runs only TestErlang* classes.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')
        timeout: Per-test timeout in seconds

    Returns:
        Same as run_tests()
    """
    return run_tests(module_name, 'TestErlang*', timeout)


def run_asyncio_tests(module_name: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Run only asyncio comparison tests from a module.

    Convenience function that runs only TestAIO* classes.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')
        timeout: Per-test timeout in seconds

    Returns:
        Same as run_tests()
    """
    return run_tests(module_name, 'TestAIO*', timeout)


def list_test_classes(module_name: str) -> List[str]:
    """List all test classes in a module.

    Args:
        module_name: Fully qualified module name

    Returns:
        List of test class names
    """
    if isinstance(module_name, bytes):
        module_name = module_name.decode('utf-8')

    try:
        module = __import__(module_name, fromlist=[''])
        classes = []
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and issubclass(obj, unittest.TestCase):
                if obj is not unittest.TestCase:
                    classes.append(name)
        return classes
    except Exception:
        return []


if __name__ == '__main__':
    # Allow running from command line for debugging
    if len(sys.argv) >= 2:
        module = sys.argv[1]
        pattern = sys.argv[2] if len(sys.argv) >= 3 else '*'
        timeout = float(sys.argv[3]) if len(sys.argv) >= 4 else 30.0
        result = run_tests(module, pattern, timeout)
        print(result['output'])
        print(f"\nTests run: {result['tests_run']}")
        print(f"Failures: {result['failures']}")
        print(f"Errors: {result['errors']}")
        print(f"Skipped: {result['skipped']}")
        print(f"Success: {result['success']}")
        sys.exit(0 if result['success'] else 1)
    else:
        print("Usage: python async_test_runner.py <module_name> [pattern] [timeout]")
        print("Example: python async_test_runner.py tests.test_base TestErlang* 30")
