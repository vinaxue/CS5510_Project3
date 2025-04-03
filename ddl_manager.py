from BTrees.OOBTree import OOBTree


class DDLManager:
    """Support data definition queries: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX"""

    # TODO: handle primary keys and foreign keys

    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.index = self.storage_manager.index
        self.db = self.storage_manager.db

    def reload(self):
        """Reloads the latest data and index"""
        self.storage_manager.load_data()
        self.storage_manager.load_index()

    def create_table(self, table_name, columns, primary_key):
        """Creates a new table with specified columns and primary key."""

        # Ensure we have the latest data
        self.reload()

        if table_name in self.db["TABLES"]:
            raise ValueError(f"Table '{table_name}' already exists")

        self.db["TABLES"][table_name] = {"primary_key": primary_key}
        self.db["COLUMNS"][table_name] = columns
        self.db["DATA"][table_name] = []

        # Initialize indexes for all columns
        # Store the column indexes as a BTree
        self.index[table_name] = {col: OOBTree() for col in columns}

        self.storage_manager.save_db(self.db)
        self.storage_manager.save_index()
