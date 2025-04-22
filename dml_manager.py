from collections import defaultdict
from BTrees.OOBTree import OOBTree

from utils import DESC, DOUBLE, INT, MAX, MIN, STRING, SUM, _make_where_fn
import utils


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
        Inserts a new row into the specified table, checking for duplicate entries,
        verifying that each value matches the expected column type, and ensuring
        foreign key references exist.
        Supports types: int, string, and double (float).
        Optimized using the primary key index if available.

        :param table_name: The name of the table.
        :param row: A list representing a row of data.
        :raises ValueError: If the table does not exist, row length is incorrect,
                            a duplicate primary key is found, a column's type does not match,
                            or a foreign key reference does not exist.
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
            if row[i] is None:
                continue
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

        # Check foreign key references if defined
        if "foreign_keys" in table_def:
            foreign_keys = table_def["foreign_keys"]
            for ref in foreign_keys:
                col_name = ref[0]  # column name
                ref_table = ref[1]  # referenced table
                ref_col = ref[2]  # referenced column
                try:
                    fk_index = col_names.index(col_name)
                except ValueError:
                    raise ValueError(
                        f"Foreign key column '{col_name}' is not defined in the table columns."
                    )
                fk_value = row[fk_index]
                # Skip validation if the value is None
                if fk_value is None:
                    continue
                # Validate that the referenced value exists in the referenced table
                if ref_table not in self.db["TABLES"]:
                    raise ValueError(
                        f"Referenced table '{ref_table}' for foreign key '{col_name}' does not exist."
                    )
                ref_table_data = self.db["DATA"][ref_table]
                ref_table_columns = self.db["COLUMNS"][ref_table]
                try:
                    ref_col_index = list(ref_table_columns.keys()).index(ref_col)
                except ValueError:
                    raise ValueError(
                        f"Referenced column '{col_name}' in table '{ref_table}' does not exist."
                    )
                if not any(row[ref_col_index] == fk_value for row in ref_table_data):
                    raise ValueError(
                        f"Foreign key constraint violation: value '{fk_value}' in column '{col_name}' "
                        f"does not exist in referenced table '{ref_table}', column '{ref_col}'."
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
        self,
        table_name,
        columns=None,
        where=None,
        group_by=None,
        aggregates=None,
        having=None,
        order_by=None,
    ):
        """
        Selects rows from a table based on optional filtering and aggregation.
        :param table_name: The name of the table.
        :param columns: List of columns to select. If None, selects all columns.
        :param where: None | list | dict | callable(row_dict)->bool
        :param group_by: List of columns to group by. If None, no grouping is applied.
        :param having: Optional filtering condition after aggregation. Same types as `where`.
        :param aggregates: Dictionary of aggregate functions to apply, e.g., {"SUM": "column_name"}.
        :param order_by: tuple of columns to order by, e.g., [("name", "ASC"), ("age", "DESC")].
        :return: A list of dictionaries representing the selected rows.
        """
        self.reload()

        # Validate table existence
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        table_columns = self.db["COLUMNS"][table_name]
        col_names = list(table_columns.keys())
        table_data = self.db["DATA"][table_name]

        if callable(where):
            where_fn = where
        else:
            where_fn = _make_where_fn(where, col_names)

        filtered_rows = []
        for row_list in table_data:
            row_dict = {}
            for col, raw in zip(col_names, row_list):
                col_type = table_columns[col]
                if col_type == INT:
                    row_dict[col] = int(raw)
                elif col_type == DOUBLE:
                    row_dict[col] = float(raw)
                else:  # STRING
                    row_dict[col] = raw

            if where_fn(row_dict):
                filtered_rows.append(row_dict)

        if columns:
            filtered_rows = [
                {col: row[col] for col in columns if col in row}
                for row in filtered_rows
            ]

        group_by_res = []
        if group_by is not None:
            group_by_res = utils.group_by(filtered_rows, group_by)
        else:
            group_by_res = filtered_rows

        aggregates_res = []
        if aggregates is not None:
            aggregates_res = utils.aggregation(group_by_res, aggregates, group_by)
            if having:
                if callable(having):
                    having_fn = having
                else:
                    having_fn = _make_where_fn(having, col_names)
                aggregates_res = [row for row in aggregates_res if having_fn(row)]
        else:
            if isinstance(group_by_res, defaultdict):
                group_by_res = [
                    {
                        **{group_by[i]: group_key[i] for i in range(len(group_key))},
                        **group_rows[0],
                    }
                    for group_key, group_rows in group_by_res.items()
                ]
            aggregates_res = group_by_res

        if order_by:
            results = utils.order_by(aggregates_res, order_by)
        else:
            results = aggregates_res

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
        primary_key = self.db["TABLES"][table_name].get("primary_key")

        # 2. Wrap `where` so that if it's a dict->bool function, we convert row list to dict
        where_fn = _make_where_fn(where, col_names)

        # 3. Perform updates
        # First pass — simulate new values, check for primary key duplicates
        updated_pks = set()
        existing_pks = {row[col_idx[primary_key]] for row in data}

        for idx, row in enumerate(data):
            if where_fn is None or where_fn(row):
                new_row = row.copy()
                for col, new_value in updates.items():
                    if col not in table_columns:
                        raise ValueError(
                            f"Column '{col}' does not exist in table '{table_name}'"
                        )
                    ci = col_idx[col]
                    new_row[ci] = (
                        new_value(new_row[ci]) if callable(new_value) else new_value
                    )

                if primary_key:
                    new_pk = new_row[col_idx[primary_key]]
                    old_pk = row[col_idx[primary_key]]

                    if new_pk != old_pk:
                        if new_pk in existing_pks or new_pk in updated_pks:
                            raise ValueError(
                                f"Duplicate primary key '{new_pk}' after update."
                            )
                        updated_pks.add(new_pk)

        # Second pass — actually apply updates
        update_count = 0
        for idx, row in enumerate(data):
            if where_fn is None or where_fn(row):
                new_row = row.copy()
                for col, new_value in updates.items():
                    ci = col_idx[col]
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
        order_by=None,
        group_by=None,
        having=None,
        aggregates=None,
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

        idx_col = right_join_col if inner_table == right_table else left_join_col
        if inner_table in self.index and idx_col in self.index[inner_table]:
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

        if columns:
            results = [
                {col: row[col] for col in columns if col in row} for row in results
            ]

        group_by_res = []
        if group_by is not None:
            group_by_res = utils.group_by(results, group_by)
        else:
            group_by_res = results

        aggregates_res = []
        if (
            group_by is not None and aggregates is not None
        ):  # needs group by to eliminate ambiguity
            aggregates_res = utils.aggregation(group_by_res, aggregates, group_by)
            if having:
                if callable(having):
                    having_fn = having
                else:
                    having_fn = _make_where_fn(having, outer_cols + inner_cols)
                aggregates_res = [row for row in aggregates_res if having_fn(row)]
        else:
            if isinstance(group_by_res, defaultdict):
                group_by_res = [
                    {
                        **{group_by[i]: group_key[i] for i in range(len(group_key))},
                        **group_rows[0],
                    }
                    for group_key, group_rows in group_by_res.items()
                ]
            aggregates_res = group_by_res

        if order_by:
            results = utils.order_by(aggregates_res, order_by)
        else:
            results = aggregates_res

        return results
