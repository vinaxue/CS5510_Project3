from collections import defaultdict
from BTrees.OOBTree import OOBTree

from utils import DOUBLE, INT, MAX, MIN, STRING, SUM, _make_where_fn


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

        where_fn = _make_where_fn(where, col_names)

        filtered_rows = []
        for row_list in row_source:
            row_dict = dict(zip(col_names, row_list))
            if where_fn(row_dict):
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
        :param where:
            - None: delete all rows
            - list [col, op, val]: single simple condition
            - dict {"op":"AND"/"OR", "left":..., "right":...}:
            - callable(row_dict)->bool:
        :return: Number of rows deleted.
        """

        # 1. Reload data and validate table
        self.reload()
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        original = self.db["DATA"][table_name]
        cols_def = self.db["COLUMNS"][table_name]
        col_names = list(cols_def.keys())
        col_idx = {c: i for i, c in enumerate(col_names)}

        where_fn = _make_where_fn(where, col_names)

        new_data = []
        delete_count = 0
        for row in original:
            if where_fn(row):
                delete_count += 1
            else:
                new_data.append(row)
        self.db["DATA"][table_name] = new_data

        if table_name in self.index:

            for col, info in list(self.index[table_name].items()):
                name = (
                    info.get("name", f"{table_name}_{col}_idx")
                    if isinstance(info, dict)
                    else f"{table_name}_{col}_idx"
                )
                self.index[table_name][col] = {"tree": OOBTree(), "name": name}

            for rid, row in enumerate(new_data):
                for col, info in self.index[table_name].items():
                    tree = info["tree"]
                    val = row[col_idx[col]]
                    if val not in tree:
                        tree[val] = []
                    tree[val].append(rid)

        self.storage_manager.save_db()
        self.storage_manager.save_index()

        return delete_count

    def update(self, table_name, updates, where=None):
        self.reload()
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        table_columns = self.db["COLUMNS"][table_name]
        data = self.db["DATA"][table_name]
        col_names = list(table_columns.keys())
        col_idx = {c: i for i, c in enumerate(col_names)}

        # 2. Wrap `where` so that if it's a dict->bool function, we convert row list to dict
        where_fn = _make_where_fn(where, col_names)

        # 3. Perform updates
        update_count = 0
        for idx, row in enumerate(data):
            if where_fn is None or where_fn(row):
                new_row = row.copy()
                for col, new_value in updates.items():
                    if col not in table_columns:
                        raise ValueError(
                            f"Column '{col}' does not exist in table '{table_name}'"
                        )
                    ci = col_idx[col]
                    # support callable(new_value) or constant
                    new_row[ci] = (
                        new_value(new_row[ci]) if callable(new_value) else new_value
                    )
                data[idx] = new_row
                update_count += 1

        # 4. Rebuild indexes for this table if they exist
        if table_name in self.index:
            # reset each index entry to {"tree": OOBTree(), "name": ...}
            for col, info in list(self.index[table_name].items()):
                name = (
                    info.get("name", f"{table_name}_{col}_idx")
                    if isinstance(info, dict)
                    else f"{table_name}_{col}_idx"
                )
                self.index[table_name][col] = {"tree": OOBTree(), "name": name}
            # reinsert all rows
            for rid, row in enumerate(data):
                for col, info in self.index[table_name].items():
                    tree = info["tree"]
                    val = row[col_idx[col]]
                    if val not in tree:
                        tree[val] = []
                    tree[val].append(rid)

        # 5. Save changes
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
        Optimized SELECT JOIN: always iterate the smaller table, build index/hash on the larger.
        Supports where as None, list, dict, or callable(row_dict)->bool.
        """
        self.reload()

        if left_table not in self.db["TABLES"] or right_table not in self.db["TABLES"]:
            raise ValueError("One or both tables do not exist.")

        if left_alias is None or right_alias is None:
            if left_table == right_table:
                left_alias = f"{left_table}_L"
                right_alias = f"{right_table}_R"
            else:
                left_alias, right_alias = left_table, right_table

        Lcols = list(self.db["COLUMNS"][left_table].keys())
        Rcols = list(self.db["COLUMNS"][right_table].keys())
        Ldata = self.db["DATA"][left_table]
        Rdata = self.db["DATA"][right_table]

        try:
            Li = Lcols.index(left_join_col)
            Ri = Rcols.index(right_join_col)
        except ValueError:
            raise ValueError(
                f"Join column {left_join_col} or {right_join_col} not found."
            )

        if len(Ldata) <= len(Rdata):
            outer_data, outer_cols, outer_alias, outer_idx = (
                Ldata,
                Lcols,
                left_alias,
                Li,
            )
            inner_table, inner_data, inner_cols, inner_alias, inner_idx = (
                right_table,
                Rdata,
                Rcols,
                right_alias,
                Ri,
            )
        else:
            outer_data, outer_cols, outer_alias, outer_idx = (
                Rdata,
                Rcols,
                right_alias,
                Ri,
            )
            inner_table, inner_data, inner_cols, inner_alias, inner_idx = (
                left_table,
                Ldata,
                Lcols,
                left_alias,
                Li,
            )

        if inner_table in self.index and (
            (inner_table == right_table and right_join_col in self.index[inner_table])
            or (inner_table == left_table and left_join_col in self.index[inner_table])
        ):

            idx_col = right_join_col if inner_table == right_table else left_join_col
            btree = self.index[inner_table][idx_col]["tree"]
            inner_index = {
                key: [inner_data[rid] for rid in btree[key]] for key in btree
            }
        else:
            inner_index = {}
            for row in inner_data:
                key = row[inner_idx]
                inner_index.setdefault(key, []).append(row)

        if callable(where):
            match_fn = where
        else:
            match_fn = _make_where_fn(where, outer_cols + inner_cols)

        results = []
        for o_row in outer_data:
            key = o_row[outer_idx]
            if key not in inner_index:
                continue
            for i_row in inner_index[key]:

                j = {}
                for i, col in enumerate(outer_cols):
                    j[f"{outer_alias}.{col}"] = o_row[i]
                    j[col] = o_row[i]
                for i, col in enumerate(inner_cols):
                    j[f"{inner_alias}.{col}"] = i_row[i]
                    j[col] = i_row[i]

                if not match_fn(j):
                    continue

                if columns is None:
                    results.append(j)
                else:
                    row_out = {}
                    for c in columns:
                        if c in j:
                            row_out[c] = j[c]
                    results.append(row_out)

        return results
