from BTrees.OOBTree import OOBTree

from utils import DOUBLE, INT, STRING


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

    def select(self, table_name, columns=None, where=None):
        """
        Selects rows from a table.
        :param table_name: The name of the table.
        :param columns: A list of column names to retrieve; if None, all columns are returned.
        :param where: A callable that takes a row (list) and returns True if the row matches the condition.
        :return: A list of rows (each row is a dict mapping column names to values).
        """
        self.reload()

        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        table_columns = self.db["COLUMNS"][table_name]
        column_names = list(table_columns.keys())
        result = []

        for row in self.db["DATA"][table_name]:
            if where is None or where(row):
                if columns is None:
                    result.append(dict(zip(column_names, row)))
                else:
                    selected = {}
                    for col in columns:
                        if col not in table_columns:
                            raise ValueError(
                                f"Column '{col}' does not exist in table '{table_name}'"
                            )
                        selected[col] = row[list(table_columns.keys()).index(col)]
                    result.append(selected)

        return result

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
        """
        Updates rows in a table.
        :param table_name: The name of the table.
        :param updates: A dictionary mapping column names to new values, or to a function that computes the new value.
                        Example: {'age': lambda x: x + 1} or {'name': 'Alice'}
        :param where: A callable that takes a row and returns True if the row should be updated.
                      If None, all rows are updated.
        :return: The number of rows updated.
        """
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
                    # If new_value is a callable, compute the updated value based on the old value
                    new_row[col_index] = (
                        new_value(new_row[col_index])
                        if callable(new_value)
                        else new_value
                    )
                data[idx] = new_row
                update_count += 1

        # Rebuild indexes for this table if they exist
        if table_name in self.index:
            for col in self.index[table_name]:
                self.index[table_name][col] = OOBTree()
            for row_id, row in enumerate(data):
                for col, tree in self.index[table_name].items():
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
    ):
        """
        Optimized SELECT JOIN based on an equality condition using index or a hash table.

        :param left_table: Name of the left table.
        :param right_table: Name of the right table.
        :param left_join_col: Column in the left table used for join.
        :param right_join_col: Column in the right table used for join.
        :param columns: List of columns to return in the format "Table.Column".
                        If None, returns all columns from both tables.
        :param where: A callable that accepts a joined row (dict) and returns True if the row meets the filter condition.
                      If None, no additional filtering is applied.
        :return: A list of dictionaries representing the joined rows.
        """
        self.reload()

        # Validate tables existence
        if left_table not in self.db["TABLES"] or right_table not in self.db["TABLES"]:
            raise ValueError("One or both tables do not exist.")

        left_columns = self.db["COLUMNS"][left_table]
        right_columns = self.db["COLUMNS"][right_table]
        left_data = self.db["DATA"][left_table]
        right_data = self.db["DATA"][right_table]

        left_col_names = list(left_columns.keys())
        right_col_names = list(right_columns.keys())

        left_col_idx = left_col_names.index(left_join_col)
        right_col_idx = right_col_names.index(right_join_col)

        # Try to utilize right table's index for join if available.
        if right_table in self.index and right_join_col in self.index[right_table]:
            right_index = self.index[right_table][right_join_col]
            # Convert right_index to a dict with key: list of rows (row_id, row)
            right_index_dict = {}
            col_idx_right = list(right_columns).index(right_join_col)
            for key in right_index:

                right_index_dict[key] = []
                for row_id in right_index[key]:
                    right_index_dict[key].append((row_id, right_data[row_id]))
        else:
            # If no index exists, build a hash table from right table's join column.
            right_index_dict = {}
            col_idx_right = list(right_columns.keys()).index(right_join_col)
            for row_id, row in enumerate(right_data):
                key = row[col_idx_right]
                right_index_dict.setdefault(key, []).append((row_id, row))

        joined_results = []
        left_col_idx = list(left_columns.keys()).index(left_join_col)

        # Iterate over left table rows and find matching right rows via the hash table.
        for left_row in left_data:
            join_value = left_row[left_col_idx]
            if join_value in right_index_dict:
                for _, right_row in right_index_dict[join_value]:
                    # Build the joined row dictionary with "Table.Column" as keys.
                    joined_row = {}
                    for idx, col in enumerate(left_columns):
                        joined_row[f"{left_table}.{col}"] = left_row[idx]
                    for idx, col in enumerate(right_columns):
                        joined_row[f"{right_table}.{col}"] = right_row[idx]

                    # Apply additional filtering if a where condition is provided.
                    if where is None or where(joined_row):
                        if columns is None:
                            joined_results.append(joined_row)
                        else:
                            filtered_row = {
                                col: joined_row[col]
                                for col in columns
                                if col in joined_row
                            }
                            joined_results.append(filtered_row)
        return joined_results
