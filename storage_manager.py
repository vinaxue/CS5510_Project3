import os
import pickle
import shutil
from BTrees.OOBTree import OOBTree


class StorageManager:
 

    def __init__(self, db_file="./data/database.pkl", index_file="./data/index.pkl"):

        import sys

        sys.setrecursionlimit(10000)

        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        os.makedirs(os.path.dirname(index_file), exist_ok=True)
        self.db_file = db_file
        self.index_file = index_file

        if not os.path.exists(self.db_file):
            with open(self.db_file, "wb") as f:
                pickle.dump(
                    {"TABLES": {}, "COLUMNS": {}, "DATA": {}, "FOREIGN_KEYS": {}}, f
                )

        self.db = self.load_db()
        self.index = self.load_index()

    def load_db(self):
   
        with open(self.db_file, "rb") as f:
            return pickle.load(f)

    def save_db(self):
        with open(self.db_file, "wb") as f:
            pickle.dump(self.db, f)

    def load_index(self):

        if not os.path.exists(self.index_file):
            return {}

        with open(self.index_file, "rb") as f:
            flat = pickle.load(f)

        idx = {}

        for table, cols in flat.items():
            idx.setdefault(table, {})
            for col, rawdict in cols.items():
                tree = OOBTree()
                tree_data = rawdict["tree"]
                name = rawdict["name"]
                for key, rids in tree_data.items():
                    tree[key] = rids
                idx[table][col] = {"tree": tree, "name": name}
        return idx

    def save_index(self):
      
        flat = {}
        for table, cols in self.index.items():
            flat.setdefault(table, {})
            for col, info in cols.items():
                flat[table][col] = {
                    "tree": dict(info["tree"]),
                    "name": info["name"],
                }

        tmp = self.index_file + ".tmp"
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "wb") as f:
            pickle.dump(flat, f, protocol=pickle.HIGHEST_PROTOCOL)
        shutil.move(tmp, self.index_file)
