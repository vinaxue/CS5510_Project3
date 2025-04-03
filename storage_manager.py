import os
import pickle
from BTrees.OOBTree import OOBTree


class StorageManager:
    """Store tables, data, and index using BTree"""

    # TODO: handle SORT BY (probably will be in optimizer later)

    def __init__(self, db_file="./data/database.dat", index_file="./data/index.db"):
        self.db_file = db_file
        self.index_file = index_file

        # Create database file if not exist
        if not os.path.exists(self.db_file):
            with open(self.db_file, "wb") as f:
                pickle.dump(
                    {"TABLES": {}, "COLUMNS": {}, "DATA": {}, "FOREIGN_KEYS": {}}, f
                )  # System tables

        # Initialize index (B-Tree)
        self.index = self.load_index()
        self.db = self.load_db()

    def load_index(self):
        """Loads the B-Tree index for fast lookups"""
        if os.path.exists(self.index_file):
            with open(self.index_file, "rb") as f:
                return pickle.load(f)
        else:
            return {}

    def save_index(self):
        """Saves the current B-Tree index to disk"""
        with open(self.index_file, "wb") as f:
            pickle.dump(self.index, f)

    def load_db(self):
        """Loads the database file"""
        with open(self.db_file, "rb") as f:
            return pickle.load(f)

    def save_db(self, db):
        """Saves the database file"""
        with open(self.db_file, "wb") as f:
            pickle.dump(db, f)
