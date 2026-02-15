"""
Reentrant callback demonstration.

This module shows how Python code can seamlessly call Erlang functions
that in turn call back into Python - all without any special handling.
The suspension/resume mechanism is completely transparent.
"""

def fibonacci(n):
    """
    Calculate Fibonacci number using Erlang for memoization.

    This function calls Erlang's 'fib_cache' which may call back into
    Python's fibonacci() - demonstrating reentrant callbacks.
    """
    if n <= 1:
        return n

    # Call Erlang to get cached value or compute via callback
    import erlang
    return erlang.call('fib_cache', n)


def process_items(items):
    """
    Process items using an Erlang-registered validator.

    Each item is sent to Erlang for validation, which may call
    back into Python for complex validation logic.
    """
    import erlang
    results = []
    for item in items:
        # Erlang validates and may call Python's validate_item()
        result = erlang.call('validate_and_transform', item)
        results.append(result)
    return results


def validate_item(item):
    """Called by Erlang during validation - can itself call Erlang."""
    import erlang

    if not isinstance(item, dict):
        return {'valid': False, 'error': 'not a dict'}

    # Call Erlang to check against rules database
    # This creates Python -> Erlang -> Python -> Erlang chain
    rules_valid = erlang.call('check_rules', item.get('type', 'unknown'))

    if not rules_valid:
        return {'valid': False, 'error': 'failed rules check'}

    return {'valid': True, 'data': item}


def transform_data(data):
    """
    Transform data with Erlang assistance.

    Demonstrates that reentrant calls work in complex expressions:
    the erlang.call() result is used directly in arithmetic.
    """
    import erlang

    # This expression: erlang.call() + 10 works correctly because
    # the suspension mechanism properly interrupts Python execution
    multiplier = erlang.call('get_multiplier', data.get('type', 'default')) + 10

    return {
        'original': data,
        'multiplied_value': data.get('value', 0) * multiplier,
        'processed': True
    }


class DataProcessor:
    """A class demonstrating reentrant callbacks in OOP context."""

    def __init__(self, name):
        self.name = name
        self.call_count = 0

    def process(self, value):
        """Process a value, potentially triggering reentrant callbacks."""
        import erlang

        self.call_count += 1

        # Call Erlang which may call back to our other methods
        result = erlang.call('processor_handle', self.name, value)

        return {
            'processor': self.name,
            'call_count': self.call_count,
            'result': result
        }

    def compute(self, x, y):
        """Called by Erlang during processing."""
        import erlang

        # Nested: Python -> Erlang -> Python -> Erlang -> Python
        factor = erlang.call('get_factor', self.name)
        return (x + y) * factor


# Simple functions for basic testing
def double(x):
    """Simple function to double a value."""
    return x * 2


def add_one(x):
    """Add one using Erlang's double function - tests reentrant arithmetic."""
    import erlang
    # erlang.call('double_via_python', x) calls back to double()
    # Then we add 1 - this tests that suspension works in expressions
    return erlang.call('double_via_python', x) + 1


def nested_compute(n, depth):
    """
    Compute with nested Erlang callbacks up to specified depth.

    Each level calls Erlang which calls back into Python until
    we reach the target depth. Tests deep reentrant nesting.
    """
    import erlang

    if depth <= 0:
        return n

    # Call Erlang which will call nested_compute(n+1, depth-1)
    return erlang.call('nested_step', n, depth)
