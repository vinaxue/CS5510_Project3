from BTrees.OOBTree import OOBTree


class DDLManager:
    """Support data definition queries: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX"""

    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.index = self.storage_manager.index
        self.db = self.storage_manager.db

    def reload(self):
        """Reloads the latest data and index"""
        self.storage_manager.load_data()
        self.storage_manager.load_index()

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

        # Initialize indexes for all columns
        # Store the column indexes as a BTree
        self.index[table_name] = {col: OOBTree() for col in columns}

        # Store foreign key relationships
        if foreign_keys:
            self.db["FOREIGN_KEYS"][table_name] = foreign_keys

        # Save the database and index state
        self.storage_manager.save_db(self.db)
        self.storage_manager.save_index()
