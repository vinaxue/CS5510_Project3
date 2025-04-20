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

        return result, execution_time

    return wrapper


def eval_cond(cond, row, col_idx):
    c, op, v = cond
    val = row[col_idx[c]] if isinstance(row, list) else row[c]
    if op == "=":
        return val == v
    if op == "!=":
        return val != v
    if op == "<":
        return val < v
    if op == ">":
        return val > v
    raise ValueError(f"Unsupported operator '{op}'")


def _make_where_fn(where, col_names):
    col_idx = {c: i for i, c in enumerate(col_names)}

    if where is None:
        return lambda row: True

    elif callable(where):
        return lambda row: where(dict(zip(col_names, row)))

    elif isinstance(where, list):

        def where_fn(row):
            return eval_cond(where, row, col_idx)

        return where_fn

    elif isinstance(where, dict) and where.get("op") in ("AND", "OR"):

        def where_fn(row):
            L = eval_cond(where["left"], row, col_idx)
            R = eval_cond(where["right"], row, col_idx)
            return (L and R) if where["op"] == "AND" else (L or R)

        return where_fn

    else:
        raise ValueError(f"Unsupported where type: {where!r}")


STRING = "string"
INT = "int"
DOUBLE = "double"

MAX = "max"
MIN = "min"
SUM = "sum"
