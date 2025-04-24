import os
import pickle
import shutil
from BTrees.OOBTree import OOBTree

class StorageManager:
    """Store tables, data, and index using BTree in-memory, but flatten to dict on disk."""

    def __init__(self, db_file="./data/database.pkl", index_file="./data/index.pkl"):
    
        import sys
        sys.setrecursionlimit(10000)

        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        os.makedirs(os.path.dirname(index_file), exist_ok=True)
        self.db_file    = db_file
        self.index_file = index_file

     
        if not os.path.exists(self.db_file):
            with open(self.db_file, "wb") as f:
                pickle.dump(
                    {"TABLES": {}, "COLUMNS": {}, "DATA": {}, "FOREIGN_KEYS": {}}, f
                )

   
        self.db    = self.load_db()

    def load_db(self):
        """Loads the entire database from a single pickle file."""
        with open(self.db_file, "rb") as f:
            return pickle.load(f)

    def save_db(self):
        """Dumps the entire database to a single pickle file."""
        with open(self.db_file, "wb") as f:
            pickle.dump(self.db, f)

    def load_index(self):
        """
        Loads the flattened index dict from disk, and rebuilds OOBTrees in memory.
        On disk we store a dict:
            { table: { column: { key: [row_id, ...], ... }, ... }, ... }
        Here we read that, then for each (table,column) build a new OOBTree.
        """
        if not os.path.exists(self.index_file):
            return {}

     
        with open(self.index_file, "rb") as f:
            flat = pickle.load(f)

        idx = {}
     
        for table, cols in flat.items():
            idx.setdefault(table, {})
            for col, rawdict in cols.items():
                tree = OOBTree()
                for key, rids in rawdict.items():
                    tree[key] = rids
                idx[table][col] = {
                    "tree": tree,
                    "name": f"{table}_{col}_idx"
                }
        return idx

    def save_index(self):
        """
        Flattens each in-memory OOBTree into a plain dict and writes one file.
        Structure on disk:
           { table: { column: { key: [row_id, ...], ... }, ... }, ... }
        """
 
        flat = {}
        for table, cols in self.index.items():
            flat.setdefault(table, {})
            for col, info in cols.items():
                flat[table][col] = dict(info["tree"])

        tmp = self.index_file + ".tmp"
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "wb") as f:
            pickle.dump(flat, f, protocol=pickle.HIGHEST_PROTOCOL)
        shutil.move(tmp, self.index_file)
