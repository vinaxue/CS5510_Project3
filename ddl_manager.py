from BTrees.OOBTree import OOBTree

from utils import DOUBLE, INT, STRING


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

    def create_index(self, table_name, column_name, index_name=None):
        """Creates (or recreates) an index on a specified column of a table."""

        self.reload()


        if table_name not in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' does not exist")
        if column_name not in self.db["COLUMNS"][table_name]:
            raise ValueError(
                f"Column '{column_name}' does not exist in table '{table_name}'"
            )

        if table_name not in self.index:
            self.index[table_name] = {}

 
        if column_name in self.index[table_name]:

            old = self.index[table_name][column_name]
            self.index[table_name][column_name] = {
                "tree": OOBTree(),
                "name": old.get("name", index_name or f"{table_name}_{column_name}_idx"),
            }
        else:

            self.index[table_name][column_name] = {
                "tree": OOBTree(),
                "name": index_name or f"{table_name}_{column_name}_idx",
            }

        col_names = list(self.db["COLUMNS"][table_name].keys())
        col_idx   = col_names.index(column_name)
        tree      = self.index[table_name][column_name]["tree"]

        for row_id, row in enumerate(self.db["DATA"][table_name]):
            key = row[col_idx]
            tree.setdefault(key, []).append(row_id)

       
        self.storage_manager.save_index()

    def drop_index(self, index_name):
        """Drops an index by its name"""
        self.reload()

        found = False

        for table_name in list(self.index.keys()):
            for column_name in list(self.index[table_name].keys()):
                index_info = self.index[table_name][column_name]
                if index_info["name"] == index_name:
                    # Drop the index
                    del self.index[table_name][column_name]
                    found = True
                    break
            if found:
                break

        if not found:
            raise ValueError(f"No index found with the name '{index_name}'")

        self.storage_manager.save_index()

    def create_table(self, table_name, columns, primary_key, foreign_keys=None):
        """
        Creates a new table with specified columns, primary key, and foreign keys.
        `columns` should be a list of tuples like [('id', 'INT'), ('name', 'STRING'), ('age', 'INT')]
        foreign_keys should be a list of tuples, e.g., [('column_name', 'referenced_table', 'referenced_column')]
        """
        # Ensure we have the latest data
        self.reload()

        # Check for duplicates
        if table_name in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' already exists")

        # Validate column types
        valid_types = {INT, STRING, DOUBLE}
        for col_name, col_type in columns:
            if col_type not in valid_types:
                raise ValueError(
                    f"Invalid column type '{col_type}' for column '{col_name}'. Valid types are: {valid_types}"
                )

        # Add table to the database
        self.db["TABLES"][table_name] = {
            "primary_key": primary_key,
            "foreign_keys": foreign_keys or [],
        }
        self.db["COLUMNS"][table_name] = {
            col_name: col_type for col_name, col_type in columns
        }
        self.db["DATA"][table_name] = []

        # Initialize index dictionary for the table
        self.index[table_name] = {}

        # Create index only for the primary key
        self.create_index(table_name, primary_key)
        # Validate foreign key references
        if foreign_keys:
            for _, ref_table, ref_col in foreign_keys:
                if ref_table not in self.db["TABLES"]:
                    raise ValueError(
                        f"Referenced table '{ref_table}' in foreign key does not exist"
                    )
                if ref_col not in self.db["COLUMNS"][ref_table]:
                    raise ValueError(
                        f"Referenced column '{ref_col}' in table '{ref_table}' does not exist"
                    )

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
