from collections import defaultdict
import time
import functools

STRING = "string"
INT = "int"
DOUBLE = "double"

MAX = "max"
MIN = "min"
SUM = "sum"

DESC = "desc"
ASC = "asc"


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


def aggregation(results, group_by, aggregates):
    """Performs aggregation on the results based on group_by and aggregates."""
    groups = defaultdict(list)
    for row in results:
        key = tuple(row[col] for col in group_by)
        groups[key].append(row)

    aggregated_results = []

    for group_key, group_rows in groups.items():
        result_row = {col: group_key[i] for i, col in enumerate(group_by)}

        for agg_func, col in aggregates.items():
            values = [r[col] for r in group_rows if r[col] is not None]

            if not values:
                result_row[col] = None
            elif agg_func == MAX:
                result_row[col] = round(max(values), 2)
            elif agg_func == MIN:
                result_row[col] = round(min(values), 2)
            elif agg_func == SUM:
                result_row[col] = round(sum(values), 2)
            else:
                raise ValueError(f"Unsupported aggregate: {agg_func}")

        aggregated_results.append(result_row)

    return aggregated_results


def order_by(results, order_by):
    """Sorts the results based on the specified order_by criteria."""
    for col, direction in reversed(order_by):  # reversed for stable multi-key sort
        reverse = str(direction).upper() == "DESC"
        results.sort(key=lambda row: row.get(col), reverse=reverse)

    return results
