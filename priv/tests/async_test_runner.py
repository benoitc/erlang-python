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
Test runner for ErlangEventLoop tests.

Tests create their own isolated ErlangEventLoop via erlang.new_event_loop()
and manage it directly. Each test's loop has its own capsule for proper
timer and FD event routing.

Usage from Erlang:
    {ok, Results} = py:call(Ctx, 'tests.async_test_runner', run_tests,
                            [<<"tests.test_base">>, <<"TestErlang*">>]).

Test Flow:
    run_tests() runs synchronously
        │
        └─→ For each test:
                test.setUp() creates self.loop = erlang.new_event_loop()
                    │
                    └─→ Isolated ErlangEventLoop with own capsule
                │
                test runs using self.loop.run_until_complete()
                    │
                    └─→ Timers route to this loop's capsule
                │
                test.tearDown() closes self.loop
"""

import fnmatch
import io
import sys
import traceback
import unittest
from typing import Dict, Any, List


def run_test_method(test_case, method_name: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Run a single test method.

    Args:
        test_case: The test case class
        method_name: Name of the test method to run
        timeout: Per-test timeout (relies on CT for enforcement)

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
        test.setUp()
        getattr(test, method_name)()
    except unittest.SkipTest as e:
        result['status'] = 'skipped'
        result['error'] = str(e)
    except AssertionError:
        result['status'] = 'failure'
        result['error'] = traceback.format_exc()
    except Exception:
        result['status'] = 'error'
        result['error'] = traceback.format_exc()
    finally:
        try:
            test.tearDown()
        except Exception:
            pass

    return result


def run_test_class(test_class, timeout: float = 30.0) -> List[Dict[str, Any]]:
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
        result = run_test_method(test_class, method_name, timeout)
        results.append(result)

    return results


def run_tests(module_name: str, pattern: str, timeout: float = 30.0) -> Dict[str, Any]:
    """
    Run tests matching pattern synchronously.

    Each test creates its own isolated ErlangEventLoop via erlang.new_event_loop()
    and manages it directly. Tests use self.loop.run_until_complete() which
    works correctly because each loop has its own capsule for timer routing.

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

    try:
        module = __import__(module_name, fromlist=[''])
        all_results = []

        # Find all test classes matching pattern
        for name in dir(module):
            if fnmatch.fnmatch(name, pattern):
                obj = getattr(module, name)
                if isinstance(obj, type) and issubclass(obj, unittest.TestCase):
                    if obj is not unittest.TestCase:
                        results = run_test_class(obj, timeout)
                        all_results.extend(results)

        # Aggregate results
        tests_run = len(all_results)
        failures = sum(1 for r in all_results if r['status'] == 'failure')
        errors = sum(1 for r in all_results if r['status'] in ('error', 'timeout'))
        skipped = sum(1 for r in all_results if r['status'] == 'skipped')

        # Build failure details for CT reporting
        failure_details = []
        for r in all_results:
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
            'results': all_results,
            'output': _format_results(all_results),
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
