import os
import pickle
import unittest
from storage_manager import (
    StorageManager,
)
from BTrees.OOBTree import OOBTree


class TestStorageManager(unittest.TestCase):
    def setUp(self):
        """Create a temporary database file for testing"""
        self.db_file = "./test_database.dat"
        self.index_file = "./test_index.db"
        self.storage = StorageManager(self.db_file, self.index_file)

    def tearDown(self):
        """Remove test database files after each test"""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.index_file):
            os.remove(self.index_file)

    def test_database_initialization(self):
        """Test if the database initializes correctly"""
        db = self.storage.load_db()
        self.assertIn("TABLES", db)
        self.assertIn("COLUMNS", db)
        self.assertIn("DATA", db)

    def test_index_initialization(self):
        """Test if the index initializes as an empty dictionary"""
        self.assertEqual(self.storage.index, {})

    def test_save_and_load_db(self):
        """Test saving and loading the database"""
        db = self.storage.db
        db["TABLES"]["test_table"] = {"name": "test_table"}
        self.storage.save_db()
        loaded_db = self.storage.load_db()
        self.assertEqual(loaded_db["TABLES"], {"test_table": {"name": "test_table"}})

    def test_save_and_load_index(self):
        """Test saving and loading the index"""
        expected = {
            "id": {
                "tree": OOBTree(),
                "name": "test_table_id_idx",
            },
            "name": {
                "tree": OOBTree(),
                "name": "test_table_name_idx",
            },
        }
        self.storage.index["test_table"] = expected
        self.storage.save_index()
        self.storage.index = {}  # Clear in-memory index
        self.storage.index = self.storage.load_index()
        self.assertIn("id", self.storage.index["test_table"])
        self.assertIn("name", self.storage.index["test_table"])
        self.assertEqual(
            self.storage.index["test_table"]["id"]["name"], "test_table_id_idx"
        )
        self.assertEqual(
            self.storage.index["test_table"]["name"]["name"], "test_table_name_idx"
        )
        self.assertIsInstance(self.storage.index["test_table"]["id"]["tree"], OOBTree)
        self.assertIsInstance(self.storage.index["test_table"]["name"]["tree"], OOBTree)


if __name__ == "__main__":
    unittest.main()
