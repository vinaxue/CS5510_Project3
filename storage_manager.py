import os
import pickle
import shutil
import sys
from BTrees.OOBTree import OOBTree


class StorageManager:
    """Store tables, data, and index using BTree"""

    # TODO: handle SORT BY (probably will be in optimizer later)

    def __init__(self, db_file="./data/database.pkl", index_file="./data/index.pkl"):
        # print(sys.getrecursionlimit())

        max_rec = 10000
        sys.setrecursionlimit(max_rec)

        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        os.makedirs(os.path.dirname(index_file), exist_ok=True)
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
        tmp_file = self.index_file + ".tmp"
        os.makedirs(os.path.dirname(tmp_file), exist_ok=True)

        with open(tmp_file, "wb") as f:
            pickle.dump(self.index, f)
        shutil.move(tmp_file, self.index_file)

    def load_db(self):
        """Loads the database file"""
        with open(self.db_file, "rb") as f:
            return pickle.load(f)

    def save_db(self):
        """Saves the database file"""
        with open(self.db_file, "wb") as f:
            pickle.dump(self.db, f)
