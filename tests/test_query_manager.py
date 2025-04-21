import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager
from dml_manager import DMLManager
from query_manager import QueryManager
from utils import DOUBLE, INT, STRING


class TestQueryManager(unittest.TestCase):
    def setUp(self):
        """Set up a test environment with a temporary database and managers"""
        self.db_file = "./test_query_db.dat"
        self.index_file = "./test_query_index.db"
        self.storage = StorageManager(self.db_file, self.index_file)
        self.ddl_manager = DDLManager(self.storage)
        self.dml_manager = DMLManager(self.storage)
        self.query_manager = QueryManager(
            self.storage, self.ddl_manager, self.dml_manager
        )

    def tearDown(self):
        """Clean up test files"""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.index_file):
            os.remove(self.index_file)

    ########################## Helper Functions ##########################
    def setup_table_users(self):
        """Ensure the Users table exists"""
        db = self.storage.load_db()
        if "Users" not in db["TABLES"]:
            self.ddl_manager.create_table(
                "Users",
                [("UserID", INT), ("UserName", STRING), ("Email", STRING)],
                primary_key="UserID",
            )

    def setup_table_orders(self):
        """Ensure the Orders table exists"""
        self.setup_table_users()
        db = self.storage.load_db()
        if "Orders" not in db["TABLES"]:
            self.ddl_manager.create_table(
                "Orders",
                [
                    ("OrderID", INT),
                    ("OrderDate", STRING),
                    ("Amount", DOUBLE),
                    ("UserID", INT),
                ],
                primary_key="OrderID",
                foreign_keys=[("UserID", "Users", "UserID")],
            )

    def insert_user(self, user_id, user_name, email):
        """Insert a user into the Users table"""
        self.setup_table_users()
        db = self.storage.load_db()
        if [user_id, user_name, email] not in db.get("DATA", {}).get("Users", []):
            self.dml_manager.insert("Users", [user_id, user_name, email])

    def insert_order(self, order_id, order_date, amount, user_id):
        """Insert an order into the Orders table"""
        self.setup_table_orders()
        db = self.storage.load_db()
        if [order_id, order_date, amount, user_id] not in db.get("DATA", {}).get(
            "Orders", []
        ):
            self.dml_manager.insert("Orders", [order_id, order_date, amount, user_id])

    def create_index_on_users(self, column_name):
        """Create an index on the Users table"""
        self.setup_table_users()
        index = self.storage.load_index()
        if f"Users_idx_{column_name}" not in index.get("Users", {}):
            self.ddl_manager.create_index("Users", column_name)

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
            {"UserID": INT, "UserName": STRING, "Email": STRING},
        )

    def test_execute_create_table_foreign_key_query(self):
        self.setup_table_users()

        db = self.storage.load_db()

        query = "CREATE TABLE Orders (OrderID INT PRIMARY KEY, OrderDate STRING, Amount DOUBLE, UserID INT FOREIGN KEY REFERENCES Users(UserID))"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        self.assertIn("Orders", db["TABLES"])
        self.assertEqual(db["TABLES"]["Orders"]["primary_key"], "OrderID")
        self.assertEqual(
            db["COLUMNS"]["Orders"],
            {
                "OrderID": INT,
                "OrderDate": STRING,
                "Amount": DOUBLE,
                "UserID": INT,
            },
        )
        self.assertIn("Orders", db["FOREIGN_KEYS"])
        self.assertEqual(
            db["FOREIGN_KEYS"]["Orders"],
            {"UserID": {"referenced_table": "Users", "referenced_column": "UserID"}},
        )

    ############################ INSERT ##########################
    def test_execute_insert_query(self):
        self.setup_table_users()

        db = self.storage.load_db()
        index = self.storage.load_index()
        query = "INSERT INTO Users (UserID, UserName, Email) VALUES (1, 'Alice', 'alice@example.com')"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertIn("Users", db["DATA"])
        self.assertEqual(len(db["DATA"]["Users"]), 1)
        self.assertEqual(db["DATA"]["Users"][0], [1, "Alice", "alice@example.com"])
        self.assertIn("Users", index)
        self.assertIn("UserID", index["Users"])
        self.assertIn(1, index["Users"]["UserID"]["tree"])
        self.assertEqual(index["Users"]["UserID"]["tree"][1], [0])

    def test_execute_multiple_insert_query(self):
        self.insert_user(1, "Alice", "alice@example.com")

        db = self.storage.load_db()
        index = self.storage.load_index()

        query = """INSERT INTO Users (UserID, UserName, Email) VALUES (2, 'Bob', 'bob@example.com'); 
        INSERT INTO Users (UserID, UserName, Email) VALUES (3, 'Charlie', 'charlie@example.com');"""
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertIn("Users", db["DATA"])
        self.assertEqual(len(db["DATA"]["Users"]), 3)
        self.assertEqual(db["DATA"]["Users"][1], [2, "Bob", "bob@example.com"])
        self.assertEqual(db["DATA"]["Users"][2], [3, "Charlie", "charlie@example.com"])

        self.assertIn("Users", index)
        self.assertIn("UserID", index["Users"])
        self.assertIn(2, index["Users"]["UserID"]["tree"])
        self.assertIn(3, index["Users"]["UserID"]["tree"])

        self.assertEqual(index["Users"]["UserID"]["tree"][2], [1])
        self.assertEqual(index["Users"]["UserID"]["tree"][3], [2])

    def test_execute_insert_with_foreign_key_query(self):
        self.setup_table_users()
        self.setup_table_orders()
        self.insert_user(1, "Alice", "alice@example.com")

        db = self.storage.load_db()
        index = self.storage.load_index()

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
        # print(index["Orders"])
        self.assertIn("Orders", index)
        self.assertIn("OrderID", index["Orders"])
        self.assertIn(1, index["Orders"]["OrderID"]["tree"])
        self.assertEqual(index["Orders"]["OrderID"]["tree"][1], [0])

    def test_execute_insert_with_wrong_data_type(self):
        self.setup_table_users()

        query = "INSERT INTO Users (UserID, UserName, Email) VALUES ('2', 'Bob', 'bob@example.com')"
        with self.assertRaises(ValueError):
            self.query_manager.execute_query(query)

    ############################# CREATE INDEX ##########################
    def test_execute_create_index_query(self):
        self.setup_table_users()

        db = self.storage.load_db()
        index = self.storage.load_index()

        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        index = self.storage.load_index()
        self.assertIn("UserName", index["Users"])
        self.assertIn("idx_UserName", index["Users"]["UserName"]["name"])

    ############################## SELECT ##########################
    def test_execute_select_query(self):
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")

        query = "SELECT * FROM Users"
        result = self.query_manager.execute_query(query)

        self.assertEqual(
            result, [{"UserID": 1, "UserName": "Alice", "Email": "alice@example.com"}]
        )

    def test_execute_select_query_with_condition(self):
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")

        query = "SELECT UserName FROM Users WHERE UserID > 1"
        result = self.query_manager.execute_query(query)
        print(result)
        self.assertEqual(result, [{"UserName": "Bob"}])

    def test_execute_select_query_with_two_conditions(self):
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")
        self.insert_user(3, "Charlie", "charlie@example.com")

        query = "SELECT UserName FROM Users WHERE UserID < 2 OR UserName = 'Bob'"
        result = self.query_manager.execute_query(query)

        self.assertEqual(result, [{"UserName": "Alice"}, {"UserName": "Bob"}])

        query = "SELECT UserName FROM Users WHERE UserID < 3 AND UserID > 1"
        result = self.query_manager.execute_query(query)

        self.assertEqual(result, [{"UserName": "Bob"}])

    def test_execute_select_query_with_join(self):
        self.setup_table_users()
        self.setup_table_orders()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")
        self.insert_order(1, "2023-10-01", 100.0, 1)
        self.insert_order(2, "2023-10-02", 200.0, 2)

        query = "SELECT Users.UserName, Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID"
        result = self.query_manager.execute_query(query)

        self.assertEqual(
            result,
            [
                {"Users.UserName": "Alice", "Orders.OrderID": 1},
                {"Orders.OrderID": 2, "Users.UserName": "Bob"},
            ],
        )

    def test_execute_select_query_with_join_with_conditions(self):
        self.setup_table_users()
        self.setup_table_orders()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")
        self.insert_order(1, "2023-10-01", 100.0, 1)
        self.insert_order(2, "2023-10-02", 200.0, 2)

        query = "SELECT Users.UserName, Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID WHERE Users.UserID < 2 and Orders.OrderID = 1"
        result = self.query_manager.execute_query(query)

        self.assertEqual(
            result,
            [{"Users.UserName": "Alice", "Orders.OrderID": 1}],
        )

    def test_execute_select_query_with_join_with_two_conditions(self):
        self.setup_table_users()
        self.setup_table_orders()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")
        self.insert_order(1, "2023-10-01", 100.0, 1)
        self.insert_order(2, "2023-10-02", 200.0, 2)
        self.insert_order(3, "2023-10-03", 10.0, 1)

        query = "SELECT Users.UserName, Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID WHERE Users.UserID < 2 AND Orders.Amount > 50.0"
        result = self.query_manager.execute_query(query)

        self.assertEqual(
            result,
            [{"Users.UserName": "Alice", "Orders.OrderID": 1}],
        )

        query = "SELECT Users.UserName, Orders.OrderID FROM Users JOIN Orders ON Users.UserID = Orders.UserID WHERE Users.UserID < 2 OR Orders.Amount > 50.0"
        result = self.query_manager.execute_query(query)
        print(result)
        self.assertEqual(
            result,
            [
                {"Users.UserName": "Alice", "Orders.OrderID": 1},
                {"Users.UserName": "Alice", "Orders.OrderID": 3},
                {"Users.UserName": "Bob", "Orders.OrderID": 2},
            ],
        )

    def test_execute_select_with_order_by(self):
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")
        self.setup_table_orders()
        self.insert_order(1, "2023-10-01", 100.0, 1)
        self.insert_order(2, "2023-10-02", 200.0, 1)
        self.insert_order(3, "2023-10-03", 50.0, 1)
        self.insert_order(4, "2023-10-04", 150.0, 1)
        self.insert_order(5, "2023-10-05", 50.0, 1)

        query = "SELECT * FROM Orders ORDER BY Amount DESC"
        result = self.query_manager.execute_query(query)

        expected_result = [
            {
                "OrderID": 2,
                "OrderDate": "2023-10-02",
                "Amount": 200.0,
                "UserID": 1,
            },
            {
                "OrderID": 4,
                "OrderDate": "2023-10-04",
                "Amount": 150.0,
                "UserID": 1,
            },
            {
                "OrderID": 1,
                "OrderDate": "2023-10-01",
                "Amount": 100.0,
                "UserID": 1,
            },
            {
                "OrderID": 3,
                "OrderDate": "2023-10-03",
                "Amount": 50.0,
                "UserID": 1,
            },
            {
                "OrderID": 5,
                "OrderDate": "2023-10-05",
                "Amount": 50.0,
                "UserID": 1,
            },
        ]
        self.assertEqual(result, expected_result)

        query = "SELECT * FROM Orders ORDER BY Amount ASC, OrderID DESC"
        result = self.query_manager.execute_query(query)

        expected_result = [
            {
                "OrderID": 5,
                "OrderDate": "2023-10-05",
                "Amount": 50.0,
                "UserID": 1,
            },
            {
                "OrderID": 3,
                "OrderDate": "2023-10-03",
                "Amount": 50.0,
                "UserID": 1,
            },
            {
                "OrderID": 1,
                "OrderDate": "2023-10-01",
                "Amount": 100.0,
                "UserID": 1,
            },
            {
                "OrderID": 4,
                "OrderDate": "2023-10-04",
                "Amount": 150.0,
                "UserID": 1,
            },
            {
                "OrderID": 2,
                "OrderDate": "2023-10-02",
                "Amount": 200.0,
                "UserID": 1,
            },
        ]
        self.assertEqual(result, expected_result)

    def test_execute_select_join_with_order_by(self):
        """Test join with ordering results"""
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")
        self.insert_user(2, "Bob", "bob@example.com")
        self.setup_table_orders()
        self.insert_order(101, "2023-10-01", 99.99, 1)
        self.insert_order(102, "2023-10-02", 49.99, 1)
        self.insert_order(103, "2023-10-03", 29.99, 2)
        self.insert_order(104, "2023-10-04", 199.99, 2)

        query = "SELECT Users.UserName, Orders.Amount FROM Users JOIN Orders ON Users.UserID = Orders.UserID ORDER BY Orders.Amount DESC"
        results = self.query_manager.execute_query(query)

        expected = [
            {"Users.UserName": "Bob", "Orders.Amount": 199.99},
            {"Users.UserName": "Alice", "Orders.Amount": 99.99},
            {"Users.UserName": "Alice", "Orders.Amount": 49.99},
            {"Users.UserName": "Bob", "Orders.Amount": 29.99},
        ]

        self.assertEqual(results, expected)

    ############################### UPDATE ##########################
    def test_execute_update_query(self):
        self.setup_table_users()
        self.insert_user(1, "Alice", "alice@example.com")

        db = self.storage.load_db()
        index = self.storage.load_index()

        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        query = "UPDATE Users SET UserName = 'Alice Smith' WHERE UserID = 1 OR UserName = 'Alice'"
        self.query_manager.execute_query(query)
        db = self.storage.load_db()
        index = self.storage.load_index()

        # print(index["Users"])
        self.assertEqual(db["DATA"]["Users"][0][1], "Alice Smith")
        self.assertEqual(
            db["DATA"]["Users"][0], [1, "Alice Smith", "alice@example.com"]
        )
        self.assertIn("Alice Smith", index["Users"]["UserName"]["tree"])
        self.assertEqual(index["Users"]["UserName"]["tree"]["Alice Smith"], [0])

    ############################## DELETE ##########################
    def test_execute_delete_query(self):
        self.setup_table_users()
        self.insert_user(1, "Alice Smith", "alice@example.com")
        self.insert_user(2, "Bob Smith", "bob@example.com")
        self.insert_user(3, "Charlie Smith", "Charlie@example.com")

        db = self.storage.load_db()
        index = self.storage.load_index()

        query = "CREATE INDEX idx_UserName ON Users(UserName)"
        self.query_manager.execute_query(query)

        query = "DELETE FROM Users WHERE UserID < 1.5 OR UserName = 'Alice Smith'"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()
        # print(db["DATA"]["Users"])
        self.assertEqual(len(db["DATA"]["Users"]), 2)
        self.assertNotIn(1, index["Users"]["UserID"])
        self.assertNotIn("Alice Smith", index["Users"]["UserName"])

    ############################## DROP INDEX ##########################
    def test_execute_drop_index_query(self):
        self.setup_table_users()
        self.create_index_on_users("UserName")

        index = self.storage.load_index()
        db = self.storage.load_db()

        query = "DROP INDEX Users_UserName_idx ON Users"
        self.query_manager.execute_query(query)

        index = self.storage.load_index()

        self.assertNotIn("Users_UserName_idx", index["Users"])

    ############################# DROP TABLE ##########################
    def test_execute_drop_table_query(self):
        db = self.storage.load_db()

        self.setup_table_users()
        self.setup_table_orders()

        db = self.storage.load_db()
        index = self.storage.load_index()

        query = "DROP TABLE Orders"
        self.query_manager.execute_query(query)

        db = self.storage.load_db()
        index = self.storage.load_index()

        self.assertNotIn("Orders", db["TABLES"])
        self.assertNotIn("Orders", db["COLUMNS"])
        self.assertNotIn("Orders", db["DATA"])
        self.assertNotIn("Orders", db["FOREIGN_KEYS"])
        self.assertNotIn("Orders", index)
