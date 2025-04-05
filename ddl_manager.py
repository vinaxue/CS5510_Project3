from BTrees.OOBTree import OOBTree


class DDLManager:
    """Support data definition queries: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX"""

    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.index = self.storage_manager.index
        self.db = self.storage_manager.db

    def reload(self):
        """Reloads the latest data and index"""
        self.storage_manager.load_db()
        self.storage_manager.load_index()

    def create_index(self, table_name, column_name):
        """Creates an index on a specified column of a table"""
        self.reload()

        # Validate table and column
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")
        if column_name not in self.db["COLUMNS"][table_name]:
            raise ValueError(
                f"Column '{column_name}' does not exist in table '{table_name}'"
            )

        if table_name not in self.index:
            self.index[table_name] = {}

        if column_name not in self.index[table_name]:
            self.index[table_name][column_name] = OOBTree()

        # Populate the index with existing data if any
        col_index = self.db["COLUMNS"][table_name].index(column_name)
        for row_id, row in enumerate(self.db["DATA"][table_name]):
            value = row[col_index]
            if value not in self.index[table_name][column_name]:
                self.index[table_name][column_name][value] = []
            self.index[table_name][column_name][value].append(row_id)

        # Save index
        self.storage_manager.save_index()

    def drop_index(self, table_name, column_name):
        """Drops an index on a specified column of a table"""
        self.reload()

        # Validate table and column
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")
        if column_name not in self.db["COLUMNS"][table_name]:
            raise ValueError(
                f"Column '{column_name}' does not exist in table '{table_name}'"
            )

        # Check if the column has an index
        if table_name in self.index and column_name in self.index[table_name]:
            self.index[table_name].pop(column_name)
        else:
            raise ValueError(
                f"No index found on column '{column_name}' in table '{table_name}'"
            )

        self.storage_manager.save_index()

    def create_table(self, table_name, columns, primary_key, foreign_keys=None):
        """
        Creates a new table with specified columns, primary key, and foreign keys.
        foreign_keys should be a list of tuples, e.g., [('column_name', 'referenced_table', 'referenced_column')]
        """
        # Ensure we have the latest data
        self.reload()

        # Check for duplicates
        if table_name in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' already exists")

        # Add table to the database
        self.db["TABLES"][table_name] = {
            "primary_key": primary_key,
            "foreign_keys": foreign_keys or [],
        }
        self.db["COLUMNS"][table_name] = columns
        self.db["DATA"][table_name] = []

        # Initialize index dictionary for the table
        self.index[table_name] = {}

        # Create index only for the primary key
        self.create_index(table_name, primary_key)

        # Store foreign key relationships
        if foreign_keys:
            self.db["FOREIGN_KEYS"][table_name] = {
                col: {"referenced_table": ref_table, "referenced_column": ref_col}
                for col, ref_table, ref_col in foreign_keys
            }

        # Save the database and index state
        self.storage_manager.save_db()
        self.storage_manager.save_index()

    def drop_table(self, table_name):
        """Drops a table and removes its data and index"""
        self.reload()

        # Check if table exists
        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")

        # Check if the table is referenced in any foreign key constraints
        for tbl, fks in self.db["FOREIGN_KEYS"].items():
            for col, ref in fks.items():
                if ref["referenced_table"] == table_name:
                    raise ValueError(
                        f"Cannot drop table '{table_name}': It is referenced by '{tbl}'."
                    )

        # Remove table metadata
        self.db["TABLES"].pop(table_name, None)
        self.db["COLUMNS"].pop(table_name, None)
        self.db["DATA"].pop(table_name, None)
        self.db["FOREIGN_KEYS"].pop(table_name, None)  # Remove if exists

        # Remove table indexes
        self.index.pop(table_name, None)

        # Save changes
        self.storage_manager.save_db()
        self.storage_manager.save_index()
