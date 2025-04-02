import os
import pickle
from BTrees.OOBTree import OOBTree


class StorageManager:
    """Store tables, data, and index using BTree"""

    # TODO: add implementation to store tables as two tables - TABLES, COLUMNS
    # TODO: handle SORT BY (probably will be in optimizer later)

    def __init__(self, db_file="./data/database.dat", index_file="./data/index.db"):
        self.db_file = db_file
        self.index_file = index_file

        # Create files if not exist
        if not os.path.exists(self.db_file):
            with open(self.db_file, "wb") as f:
                pickle.dump([], f)

        # Initialize index (B-Tree)
        self.index = self.load_index()

    def load_index(self):
        """Loads the B-Tree index for fast lookups"""
        if os.path.exists(self.index_file):
            with open(self.index_file, "rb") as f:
                return pickle.load(f)  # Load the index from the file
        else:
            return OOBTree()

    def save_index(self):
        """Saves the current B-Tree index to the disk"""
        with open(self.index_file, "wb") as f:
            pickle.dump(self.index, f)

    def insert(self, record_id, record):
        """Inserts a record into storage and updates index"""
        with open(self.db_file, "rb+") as f:
            records = pickle.load(f)
            records.append(record)
            f.seek(0)
            pickle.dump(records, f)

        # Update index
        self.index[record_id] = len(records) - 1
        self.save_index()
        print(f"Inserted: {record}")

    def retrieve(self, record_id):
        """Retrieves a record using the B-Tree index"""
        if record_id in self.index:
            pos = self.index[record_id]
            with open(self.db_file, "rb") as f:
                records = pickle.load(f)
                return records[pos]
        return None


# # Usage Example
# storage = StorageManager()

# # Insert records
# storage.insert("101", {"name": "Alice", "age": 25})
# storage.insert("102", {"name": "Bob", "age": 30})

# # Retrieve records
# print(storage.retrieve("101"))  # Output: {'name': 'Alice', 'age': 25}
# print(storage.retrieve("102"))  # Output: {'name': 'Bob', 'age': 30}
