import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager
from dml_manager import DMLManager
from query_manager import QueryManager


class TestQueryManager(unittest.TestCase):
    def setUp(self):
        """Set up a test environment with a temporary database and managers"""
        self.db_file = "./test_query_db.dat"
        self.index_file = "./test_query_index.db"
        self.storage = StorageManager(self.db_file, self.index_file)
        self.ddl_manager = DDLManager(self.storage)
        self.dml_manager = DMLManager(self.storage)
        self.query_manager = QueryManager(self.ddl_manager, self.dml_manager)

    def tearDown(self):
        """Clean up test files"""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.index_file):
            os.remove(self.index_file)

    ########################## CREATE TABLE ##########################
    def test_execute_create_table_query(self):
        query = (
            "CREATE TABLE Users (UserID INT PRIMARY KEY, UserName STRING, Email STRING)"
        )
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        self.assertIn("Users", db["TABLES"])
        self.assertEqual(db["TABLES"]["Users"]["primary_key"], "UserID")
        self.assertEqual(
            db["COLUMNS"]["Users"],
            {"UserID": "INT", "UserName": "STRING", "Email": "STRING"},
        )

    def test_execute_create_table_foreign_key_query(self):
        query = "CREATE TABLE Orders (OrderID INT PRIMARY KEY, OrderDate STRING, Amount DOUBLE, UserID INT FOREIGN KEY REFERENCES Users(UserID))"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        self.assertIn("Orders", db["TABLES"])
        self.assertEqual(db["TABLES"]["Orders"]["primary_key"], "OrderID")
        self.assertEqual(
            db["COLUMNS"]["Orders"],
            {
                "OrderID": "INT",
                "OrderDate": "STRING",
                "Amount": "DOUBLE",
                "UserID": "INT",
            },
        )
        self.assertIn("FOREIGN_KEYS", db["Orders"])
        self.assertEqual(
            db["FOREIGN_KEYS"]["Orders"],
            {"UserID": {"referenced_table": "Users", "referenced_column": "UserID"}},
        )

    ############################ INSERT ##########################
    def test_execute_insert_query(self):
        db = self.storage.load_db()
        self.assertIn("Users", db["TABLES"])

        query = "INSERT INTO Users (UserID, UserName, Email) VALUES (1, 'Alice', 'alice@example.com')"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertIn("Users", db["DATA"])
        self.assertEqual(len(db["DATA"]["Users"]), 1)
        self.assertEqual(db["DATA"]["Users"][0], [1, "Alice", "alice@example.com"])

        self.assertIn("Users", index)
        self.assertIn("UserID", index["Users"])
        self.assertIn(1, index["Users"]["UserID"])

        self.assertEqual(index["Users"]["UserID"][1], [0])

    ############################# CREATE INDEX ##########################
    def test_execute_create_index_query(self):
        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        index = self.storage.load_index()

        self.assertIn("idx_UserName", index["Users"])
        self.assertIn("UserName", index["Users"]["idx_UserName"])
