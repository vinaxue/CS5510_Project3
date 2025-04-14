import time
import functools


def track_time(func):
    """Decorator to track execution time of a function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        execution_time = end_time - start_time
        print(f"Query '{func.__name__}' executed in {execution_time:.6f} seconds")

        return result

    return wrapper


STRING = "string"
INT = "int"
DOUBLE = "double"
