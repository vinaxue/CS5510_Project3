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

    def test_execute_multiple_insert_query(self):
        db = self.storage.load_db()
        self.assertIn("Users", db["TABLES"])
        len_before = len(db["DATA"].get("Users", []))

        query = """INSERT INTO Users (UserID, UserName, Email) VALUES (2, 'Bob', 'bob@example.com'); 
        INSERT INTO Users (UserID, UserName, Email) VALUES (3, 'Charlie', 'charlie@example.com');"""
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertIn("Users", db["DATA"])
        self.assertEqual(len(db["DATA"]["Users"]), len_before + 2)
        self.assertEqual(db["DATA"]["Users"][len_before], [2, "Bob", "bob@example.com"])
        self.assertEqual(
            db["DATA"]["Users"][len_before + 1], [3, "Charlie", "charlie@example.com"]
        )

        self.assertIn("Users", index)
        self.assertIn("UserID", index["Users"])
        self.assertIn(2, index["Users"]["UserID"])
        self.assertIn(3, index["Users"]["UserID"])

        self.assertEqual(index["Users"]["UserID"][2], [len_before])
        self.assertEqual(index["Users"]["UserID"][3], [len_before + 1])

    def test_execute_insert_with_foreign_key_query(self):
        db = self.storage.load_db()
        self.assertIn("Users", db["TABLES"])
        self.assertIn("Orders", db["TABLES"])

        query = "INSERT INTO Orders (OrderID, OrderDate, Amount, UserID) VALUES (1, '2023-10-01', 100.0, 1)"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertIn("Orders", db["DATA"])
        self.assertEqual(len(db["DATA"]["Orders"]), 1)
        self.assertEqual(
            db["DATA"]["Orders"][0],
            [1, "2023-10-01", 100.0, 1],
        )

        self.assertIn("Orders", index)
        self.assertIn("OrderID", index["Orders"])
        self.assertIn(1, index["Orders"]["OrderID"])

        self.assertEqual(index["Orders"]["OrderID"][1], [0])

    ############################# CREATE INDEX ##########################
    def test_execute_create_index_query(self):
        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        index = self.storage.load_index()

        self.assertIn("idx_UserName", index["Users"])
        self.assertIn("UserName", index["Users"]["idx_UserName"])

    ############################## SELECT ##########################
    def test_execute_select_query(self):
        query = "SELECT UserName FROM Users WHERE UserID = 1"
        result = self.query_manager.execute_query(query)

        self.assertEqual(result, [["Alice"]])

    def test_execute_select_query_with_condition(self):
        query = "SELECT UserName FROM Users WHERE UserID > 1"
        result = self.query_manager.execute_query(query)

        self.assertEqual(result, [["Bob"], ["Charlie"]])

    def test_execute_select_query_with_join(self):
        query = "SELECT Users.UserName, Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID"
        result = self.query_manager.execute_query(query)

        self.assertEqual(result, [["Alice", 1]])

    ############################### UPDATE ##########################
    def test_execute_update_query(self):
        query = "UPDATE Users SET UserName = 'Alice Smith' WHERE UserID = 1"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertEqual(db["DATA"]["Users"][0][1], "Alice Smith")
        self.assertIn("Alice Smith", index["Users"]["UserName"])
        self.assertNotIn("Alice", index["Users"]["UserName"])
        self.assertEqual(index["Users"]["UserName"]["Alice Smith"], [0])

    ############################## DELETE ##########################
    def test_execute_delete_query(self):
        query = "DELETE FROM Users WHERE UserID = 1"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertEqual(len(db["DATA"]["Users"]), 2)
        self.assertNotIn(1, index["Users"]["UserID"])
        self.assertNotIn("Alice Smith", index["Users"]["UserName"])

    ############################## DROP INDEX ##########################
    def test_execute_drop_index_query(self):
        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        query = "DROP INDEX idx_UserName ON Users"
        self.query_manager.execute_query(query)

        index = self.storage.load_index()

        self.assertNotIn("idx_UserName", index["Users"])

    ############################# DROP TABLE ##########################
    def test_execute_drop_table_query(self):
        query = "DROP TABLE Orders"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()

        self.assertNotIn("Orders", db["TABLES"])
        self.assertNotIn("Orders", db["COLUMNS"])
        self.assertNotIn("Orders", db["DATA"])
        self.assertNotIn("Orders", db["FOREIGN_KEYS"])
