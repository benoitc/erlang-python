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
Test runner for CT (Common Test) integration.

This module provides functions to run Python unittest tests from Erlang
and return results in a format suitable for CT reporting.

Usage from Erlang:
    {ok, Results} = py:call(Ctx, 'tests.ct_runner', run_tests,
                            [<<"tests.test_base">>, <<"TestErlang*">>]).
"""

import fnmatch
import io
import sys
import traceback
import unittest
from typing import Any, Dict, List


def run_tests(module_name: str, pattern: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Run unittest tests matching a pattern and return results.

    This function is designed to be called from Erlang via py:call().
    It discovers and runs test classes matching the given pattern,
    then returns a dictionary with test results.

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
            - output: Test runner output (string)
            - failure_details: List of failure/error details
    """
    import signal
    import threading
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

    # Handle binary strings from Erlang
    if isinstance(module_name, bytes):
        module_name = module_name.decode('utf-8')
    if isinstance(pattern, bytes):
        pattern = pattern.decode('utf-8')

    output = io.StringIO()
    loader = unittest.TestLoader()

    try:
        # Import the module
        module = __import__(module_name, fromlist=[''])

        # Find test classes matching pattern
        suite = unittest.TestSuite()
        for name in dir(module):
            if fnmatch.fnmatch(name, pattern):
                obj = getattr(module, name)
                if isinstance(obj, type) and issubclass(obj, unittest.TestCase):
                    tests = loader.loadTestsFromTestCase(obj)
                    suite.addTests(tests)

        # Create a custom test result with timeout support
        class TimeoutTestResult(unittest.TestResult):
            """Test result that wraps tests with timeout."""

            def __init__(self, stream, descriptions, verbosity, test_timeout):
                super().__init__(stream, descriptions, verbosity)
                self.stream = stream
                self.descriptions = descriptions
                self.verbosity = verbosity
                self.test_timeout = test_timeout
                self._test_executor = ThreadPoolExecutor(max_workers=1)

            def startTest(self, test):
                super().startTest(test)
                if self.verbosity > 1:
                    self.stream.write(str(test))
                    self.stream.write(" ... ")
                    self.stream.flush()

            def addSuccess(self, test):
                super().addSuccess(test)
                if self.verbosity > 1:
                    self.stream.write("ok\n")

            def addError(self, test, err):
                super().addError(test, err)
                if self.verbosity > 1:
                    self.stream.write("ERROR\n")

            def addFailure(self, test, err):
                super().addFailure(test, err)
                if self.verbosity > 1:
                    self.stream.write("FAIL\n")

            def addSkip(self, test, reason):
                super().addSkip(test, reason)
                if self.verbosity > 1:
                    self.stream.write(f"skipped {reason!r}\n")

        class TimeoutTestRunner:
            """Test runner that applies timeout to each test."""

            def __init__(self, stream, verbosity, test_timeout):
                self.stream = stream
                self.verbosity = verbosity
                self.test_timeout = test_timeout

            def run(self, test_suite):
                result = TimeoutTestResult(
                    self.stream,
                    descriptions=True,
                    verbosity=self.verbosity,
                    test_timeout=self.test_timeout
                )

                # Run each test with timeout
                for test in _iter_tests(test_suite):
                    if result.shouldStop:
                        break

                    def run_test():
                        test(result)

                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(run_test)
                        try:
                            future.result(timeout=self.test_timeout)
                        except FuturesTimeoutError:
                            # Test timed out
                            result.addError(
                                test,
                                (TimeoutError,
                                 TimeoutError(f"Test timed out after {self.test_timeout}s"),
                                 None)
                            )
                        except Exception as e:
                            # Unexpected error running test
                            result.addError(test, sys.exc_info())

                # Print summary
                self.stream.write("\n")
                self.stream.write("-" * 70)
                self.stream.write("\n")
                run = result.testsRun
                self.stream.write(f"Ran {run} test{'s' if run != 1 else ''}\n")

                return result

        def _iter_tests(suite):
            """Iterate over all tests in a suite."""
            for test in suite:
                if isinstance(test, unittest.TestSuite):
                    yield from _iter_tests(test)
                else:
                    yield test

        # Run tests with timeout support
        runner = TimeoutTestRunner(stream=output, verbosity=2, test_timeout=timeout)
        result = runner.run(suite)

        # Get output for logging
        test_output = output.getvalue()

        # Build failure details
        failure_details = []
        for test, trace in result.failures:
            failure_details.append({
                'test': str(test),
                'traceback': trace
            })
        for test, trace in result.errors:
            if isinstance(trace, tuple):
                # Format exception info
                import traceback as tb
                trace = ''.join(tb.format_exception(*trace)) if trace[2] else str(trace[1])
            failure_details.append({
                'test': str(test),
                'traceback': trace
            })

        return {
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'skipped': len(result.skipped),
            'success': result.wasSuccessful(),
            'output': test_output,
            'failure_details': failure_details
        }
    except Exception as e:
        return {
            'tests_run': 0,
            'failures': 0,
            'errors': 1,
            'skipped': 0,
            'success': False,
            'output': traceback.format_exc(),
            'failure_details': [{'test': 'import', 'traceback': str(e)}]
        }


def run_module_tests(module_name: str) -> Dict[str, Any]:
    """Run all tests in a module.

    Convenience function that runs all TestCase classes in a module.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')

    Returns:
        Same as run_tests()
    """
    return run_tests(module_name, '*')


def run_erlang_tests(module_name: str) -> Dict[str, Any]:
    """Run only Erlang event loop tests from a module.

    Convenience function that runs only TestErlang* classes.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')

    Returns:
        Same as run_tests()
    """
    return run_tests(module_name, 'TestErlang*')


def run_asyncio_tests(module_name: str) -> Dict[str, Any]:
    """Run only asyncio comparison tests from a module.

    Convenience function that runs only TestAIO* classes.

    Args:
        module_name: Fully qualified module name (e.g., 'tests.test_base')

    Returns:
        Same as run_tests()
    """
    return run_tests(module_name, 'TestAIO*')


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
    import sys
    if len(sys.argv) >= 2:
        module = sys.argv[1]
        pattern = sys.argv[2] if len(sys.argv) >= 3 else '*'
        result = run_tests(module, pattern)
        print(f"\nTests run: {result['tests_run']}")
        print(f"Failures: {result['failures']}")
        print(f"Errors: {result['errors']}")
        print(f"Skipped: {result['skipped']}")
        print(f"Success: {result['success']}")
    else:
        print("Usage: python ct_runner.py <module_name> [pattern]")
