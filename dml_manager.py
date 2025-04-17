from collections import defaultdict
from BTrees.OOBTree import OOBTree

from utils import DOUBLE, INT, MAX, MIN, STRING, SUM


class DMLManager:
    """Execute data manipulation queries: INSERT, DELETE, UPDATE, SELECT, and optimized JOIN SELECT"""

    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.db = self.storage_manager.db
        self.index = self.storage_manager.index

    def reload(self):
        """Reload the latest data and indexes"""
        self.storage_manager.load_db()
        self.storage_manager.load_index()

    def insert(self, table_name, row):
        """
        Inserts a new row into the specified table, checking for duplicate entries
        and verifying that each value matches the expected column type.
        Supports types: int, string, and double (float).
        Optimized using the primary key index if available.

        :param table_name: The name of the table.
        :param row: A list representing a row of data.
        :raises ValueError: If the table does not exist, row length is incorrect,
                            a duplicate primary key is found, or a column's type does not match.
        """
        self.reload()

        # Validate table existence
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        # Retrieve table columns (expected as list of tuples: (column_name, column_type))
        table_columns = self.db["COLUMNS"][table_name]
        # Extract column names for easier lookup later
        col_names = list(table_columns.keys())

        # Validate row length against table columns
        if len(row) != len(table_columns):
            raise ValueError("Row length does not match the number of table columns.")

        table_columns_items = list(self.db["COLUMNS"][table_name].items())
        # Check that each value's type matches the column's expected type
        for i, (col_name, col_type) in enumerate(table_columns_items):
            if col_type == INT:
                if not isinstance(row[i], int):
                    raise ValueError(
                        f"Column '{col_name}' expects type int, but got {type(row[i]).__name__}."
                    )
            elif col_type == STRING:
                if not isinstance(row[i], str):
                    raise ValueError(
                        f"Column '{col_name}' expects type string, but got {type(row[i]).__name__}."
                    )
            elif col_type == DOUBLE:
                if not isinstance(row[i], float):
                    raise ValueError(
                        f"Column '{col_name}' expects type double, but got {type(row[i]).__name__}."
                    )
            else:
                raise ValueError(
                    f"Unsupported column type '{col_type}' for column '{col_name}'."
                )

        # Check for duplicate based on primary key if defined
        table_def = self.db["TABLES"][table_name]
        if "primary_key" in table_def:
            primary_key = table_def["primary_key"]
            # Find the primary key column index using the extracted column names
            try:
                pk_index = list(self.db["COLUMNS"][table_name].keys()).index(
                    primary_key
                )
            except ValueError:
                raise ValueError(
                    f"Primary key '{primary_key}' is not defined in the table columns."
                )
            pk_value = row[pk_index]
            # Use the index if available for quick duplicate check
            if table_name in self.index and primary_key in self.index[table_name]:
                if pk_value in self.index[table_name][primary_key]["tree"]:
                    raise ValueError(
                        f"Duplicate entry for primary key '{primary_key}' with value '{pk_value}'."
                    )
            else:
                # Fallback: check in existing table data (less optimized)
                for existing_row in self.db["DATA"][table_name]:
                    if existing_row[pk_index] == pk_value:
                        raise ValueError(
                            f"Duplicate entry for primary key '{primary_key}' with value '{pk_value}'."
                        )

        # Append the new row
        self.db["DATA"][table_name].append(row)
        new_row_id = len(self.db["DATA"][table_name]) - 1

        # Update indexes if they exist
        if table_name in self.index:
            for col_name, index in self.index[table_name].items():
                try:
                    col_index = col_names.index(col_name)
                except ValueError:
                    continue  # Skip if column not found
                tree = index["tree"]
                value = row[col_index]
                if value not in tree:
                    tree[value] = []
                tree[value].append(new_row_id)

        # Save changes to the database and index storage
        self.storage_manager.save_db()
        self.storage_manager.save_index()

    def select(
        self, table_name, columns=None, where=None, group_by=None, aggregates=None
    ):
        """
        Selects rows from a table based on optional filtering and aggregation.
        :param table_name: The name of the table.
        :param columns: List of columns to select. If None, selects all columns.
        :param where: List of conditions to filter rows. If None, no filtering is applied.
            e.g. {"op": "AND", "left":["col1", "op", "val"], "right": ["col2", "op", "val"]}
            or ["col1", "op", "val"]
        :param group_by: List of columns to group by. If None, no grouping is applied.
        :param aggregates: Dictionary of aggregate functions to apply, e.g., {"SUM": "column_name"}.
        :return: A list of dictionaries representing the selected rows.
        """

        self.reload()

        table_columns = self.db["COLUMNS"][table_name]
        col_names = list(table_columns.keys())
        table_data = self.db["DATA"][table_name]
        results = []

        indexed_rows = None
        row_source = table_data

        def get_indexed_rows(cond):
            if isinstance(cond, list) and len(cond) == 3:
                col, op, val = cond
                if (
                    op == "="
                    and table_name in self.index
                    and col in self.index[table_name]
                ):
                    tree = self.index[table_name][col]["tree"]
                    if val in tree:
                        row_ids = tree[val]
                        return [table_data[i] for i in row_ids]
            return None

        if isinstance(where, dict) and where["op"] in ("AND", "OR"):
            left_rows = get_indexed_rows(where["left"])
            right_rows = get_indexed_rows(where["right"])
            if left_rows is None:
                left_rows = table_data
            if right_rows is None:
                right_rows = table_data

            if (
                where["op"] == "AND"
                and left_rows is not None
                and right_rows is not None
            ):
                left_ids = {tuple(r) for r in left_rows}
                right_ids = {tuple(r) for r in right_rows}
                intersection = left_ids & right_ids
                indexed_rows = [list(row) for row in intersection]
            elif where["op"] == "OR":
                combined_set = set()
                for cond_rows in [left_rows, right_rows]:
                    if cond_rows is not None:
                        combined_set.update(tuple(r) for r in cond_rows)
                if combined_set:
                    indexed_rows = [list(row) for row in combined_set]
        elif isinstance(where, list):
            indexed_rows = get_indexed_rows(where)

        if indexed_rows is not None:
            row_source = indexed_rows

        def eval_condition(cond, row):
            col, op, val = cond
            v = row[col]
            if op == "=":
                return v == val
            if op == "!=":
                return v != val
            if op == "<":
                return v < val
            if op == ">":
                return v > val
            return False

        def match(row):
            if where is None:
                return True
            if isinstance(where, list):
                return eval_condition(where, row)
            elif isinstance(where, dict) and where["op"] in ("AND", "OR"):
                left = eval_condition(where["left"], row)
                right = eval_condition(where["right"], row)
                return (left and right) if where["op"] == "AND" else (left or right)
            return False

        filtered_rows = []
        for row_list in row_source:
            row_dict = dict(zip(col_names, row_list))
            if match(row_dict):
                filtered_rows.append(row_dict)

        if group_by is None and aggregates is None:
            for row in filtered_rows:
                if columns:
                    results.append({col: row[col] for col in columns})
                else:
                    results.append(row)
            return results

        groups = defaultdict(list)
        for row in filtered_rows:
            key = tuple(row[col] for col in group_by)
            groups[key].append(row)

        for group_key, group_rows in groups.items():
            result_row = {col: group_key[i] for i, col in enumerate(group_by)}

            for agg_func, col in aggregates.items():
                values = [r[col] for r in group_rows if r[col] is not None]

                if not values:
                    result_row[col] = None
                elif agg_func == MAX:
                    result_row[col] = max(values)
                elif agg_func == MIN:
                    result_row[col] = min(values)
                elif agg_func == SUM:
                    result_row[col] = sum(values)
                else:
                    raise ValueError(f"Unsupported aggregate: {agg_func}")

            results.append(result_row)

        return results

    def delete(self, table_name, where=None):
        """
        Deletes rows from a table.
        :param table_name: The name of the table.
        :param where: A callable that takes a row and returns True if the row should be deleted.
                      If None, all rows will be deleted.
        :return: The number of rows deleted.
        """
        self.reload()

        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        original_data = self.db["DATA"][table_name]
        table_columns = self.db["COLUMNS"][table_name]
        new_data = []
        delete_count = 0

        # Filter rows: keep rows that do not match the 'where' condition
        for row in original_data:
            if where is None or where(row):
                delete_count += 1
            else:
                new_data.append(row)
        self.db["DATA"][table_name] = new_data

        # Rebuild indexes for this table if they exist
        if table_name in self.index:
            for col in self.index[table_name]:
                self.index[table_name][col] = OOBTree()
            for row_id, row in enumerate(new_data):
                for col, tree in self.index[table_name].items():
                    col_index = list(table_columns.keys()).index(col)
                    value = row[col_index]
                    if value not in tree:
                        tree[value] = []
                    tree[value].append(row_id)

        self.storage_manager.save_db()
        self.storage_manager.save_index()

        return delete_count

    def update(self, table_name, updates, where=None):
        self.reload()

        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        table_columns = self.db["COLUMNS"][table_name]
        data = self.db["DATA"][table_name]
        update_count = 0

        for idx, row in enumerate(data):
            if where is None or where(row):
                new_row = row.copy()
                for col, new_value in updates.items():
                    if col not in table_columns:
                        raise ValueError(
                            f"Column '{col}' does not exist in table '{table_name}'"
                        )
                    col_index = list(table_columns.keys()).index(col)
                    new_row[col_index] = (
                        new_value(new_row[col_index])
                        if callable(new_value)
                        else new_value
                    )
                data[idx] = new_row
                update_count += 1

        if table_name in self.index:

            for col, index_info in self.index[table_name].items():

                if isinstance(index_info, dict):
                    old_name = index_info.get("name", f"{table_name}_{col}_idx")

                    new_tree = OOBTree()

                    self.index[table_name][col] = {"tree": new_tree, "name": old_name}
                else:

                    new_tree = OOBTree()
                    self.index[table_name][col] = {
                        "tree": new_tree,
                        "name": f"{table_name}_{col}_idx",
                    }

            for row_id, row in enumerate(data):
                for col, index_info in self.index[table_name].items():
                    tree = index_info["tree"]
                    col_index = list(table_columns.keys()).index(col)
                    value = row[col_index]
                    if value not in tree:
                        tree[value] = []
                    tree[value].append(row_id)

        self.storage_manager.save_db()
        self.storage_manager.save_index()

        return update_count

    def select_join_with_index(
        self,
        left_table,
        right_table,
        left_join_col,
        right_join_col,
        columns=None,
        where=None,
        left_alias=None,
        right_alias=None,
    ):
        """
        Optimized SELECT JOIN based on an equality condition using an index or a hash table.

        :param left_table: Name of the left table.
        :param right_table: Name of the right table.
        :param left_join_col: Column name in the left table used for joining.
        :param right_join_col: Column name in the right table used for joining.
        :param columns: List of columns to return in the format "Alias.Column". If None, returns all columns from both tables.
        :param where: A callable that accepts a joined row (dict) and returns True if the row meets the filter condition.
                    If None, no additional filtering is applied.
        :param left_alias: Optional alias for the left table. If not provided and a self join occurs, a default alias is used.
        :param right_alias: Optional alias for the right table. If not provided and a self join occurs, a default alias is used.
        :return: A list of dictionaries representing the joined rows.
        """
        # Reload the latest database and index
        self.reload()

        # Validate that both tables exist
        if left_table not in self.db["TABLES"] or right_table not in self.db["TABLES"]:
            raise ValueError("One or both tables do not exist.")

        # If aliases are not provided, and this is a self-join, assign default aliases to differentiate the two sides.
        if left_alias is None or right_alias is None:
            if left_table == right_table:
                left_alias = f"{left_table}_L"
                right_alias = f"{right_table}_R"
            else:
                left_alias = left_table
                right_alias = right_table

        # Retrieve column definitions and data for both tables.
        left_columns_dict = self.db["COLUMNS"][left_table]
        right_columns_dict = self.db["COLUMNS"][right_table]
        left_data = self.db["DATA"][left_table]
        right_data = self.db["DATA"][right_table]

        left_columns_list = list(left_columns_dict.keys())
        right_columns_list = list(right_columns_dict.keys())

        # Get the index positions for the join columns in both tables.
        try:
            left_col_idx = left_columns_list.index(left_join_col)
            right_col_idx = right_columns_list.index(right_join_col)
        except ValueError:
            raise ValueError(
                f"Join column {left_join_col} or {right_join_col} not found."
            )

        # Use the index on the right table if available to optimize the join.
        if right_table in self.index and right_join_col in self.index[right_table]:
            right_index = self.index[right_table][right_join_col]
            right_index_dict = {}
            for key in right_index:
                row_id_list = right_index[key]
                # For each key, create a list of tuples: (row_id, corresponding row data)
                right_index_dict[key] = [(rid, right_data[rid]) for rid in row_id_list]
        else:
            # If no index exists, build a hash table using the right join column.
            right_index_dict = {}
            for row_id, row in enumerate(right_data):
                key = row[right_col_idx]
                right_index_dict.setdefault(key, []).append((row_id, row))

        joined_results = []
        # Loop through each row in the left table.
        for left_row in left_data:
            join_value = left_row[left_col_idx]
            # Check if there are matching rows in the right table via the hash table or index.
            if join_value in right_index_dict:
                for _, right_row in right_index_dict[join_value]:
                    joined_row = {}
                    # Build the left part of the joined row using the left table alias.
                    for idx, col in enumerate(left_columns_list):
                        joined_row[f"{left_alias}.{col}"] = left_row[idx]
                    # Build the right part of the joined row using the right table alias.
                    for idx, col in enumerate(right_columns_list):
                        joined_row[f"{right_alias}.{col}"] = right_row[idx]
                    # Apply the where-filter if provided.
                    if where is None or where(joined_row):
                        if columns is None:
                            joined_results.append(joined_row)
                        else:
                            filtered_row = {}
                            for c in columns:
                                if c in joined_row:
                                    filtered_row[c] = joined_row[c]
                            joined_results.append(filtered_row)
        return joined_results
