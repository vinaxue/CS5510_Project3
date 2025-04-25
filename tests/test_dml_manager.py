import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager
from dml_manager import DMLManager
from utils import ASC, DESC, DOUBLE, INT, MAX, MIN, STRING, SUM


class TestDMLManager(unittest.TestCase):
    def setUp(self):
        """Set up a test environment with a temporary database"""
        self.db_file = "./test_database.dat"
        self.index_file = "./test_index.db"
        self.storage = StorageManager(self.db_file, self.index_file)
        self.ddl_manager = DDLManager(self.storage)
        self.dml_manager = DMLManager(self.storage)

        # Create test tables
        self.ddl_manager.create_table(
            "users",
            [("id", INT), ("name", STRING), ("email", STRING)],
            primary_key="id",
        )
        self.ddl_manager.create_table(
            "orders",
            [("order_id", INT), ("user_id", INT), ("amount", DOUBLE)],
            primary_key="order_id",
        )
        self.ddl_manager.create_table(
            "products",
            [("product_id", INT), ("name", STRING), ("price", INT)],
            primary_key="product_id",
        )

    def tearDown(self):
        """Clean up test files"""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.index_file):
            os.remove(self.index_file)

    ########################## INSERT TESTS ##########################
    def test_insert_valid_row(self):
        """Test inserting a valid row"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        data = self.storage.db["DATA"]["users"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0], [1, "Alice", "alice@example.com"])

    def test_insert_duplicate_primary_key(self):
        """Test inserting a row with duplicate primary key"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        with self.assertRaises(ValueError):
            self.dml_manager.insert("users", [1, "Bob", "bob@example.com"])

    def test_insert_duplicate_row(self):
        """Test inserting a row with duplicate row"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        with self.assertRaises(ValueError):
            self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])

    def test_insert_invalid_row_length(self):
        """Test inserting a row with incorrect number of columns"""
        with self.assertRaises(ValueError):
            self.dml_manager.insert("users", [1, "Alice"])

    def test_insert_type_mismatch(self):
        """Test inserting a row with type mismatch"""
        with self.assertRaises(ValueError):
            self.dml_manager.insert("users", ["1", "Alice", "alice@example.com"])

    def test_insert_updates_index(self):
        """Test that insert operation updates the index"""
        self.dml_manager.insert("users", [1, "Bob", "bob@example.com"])
        index = self.storage.load_index()
        self.assertIn(1, index["users"]["id"]["tree"])

    def test_insert_with_foreign_key(self):
        """Test inserting a row with foreign key reference"""
        self.ddl_manager.create_table(
            "products_2",
            [("product_id", INT), ("name", STRING), ("price", INT)],
            primary_key="product_id",
        )
        self.ddl_manager.create_table(
            "order_items",
            [("order_item_id", INT), ("order_id", INT), ("product_id", INT)],
            primary_key="order_item_id",
            foreign_keys=[("product_id", "products_2", "product_id")],
        )
        self.dml_manager.insert("products_2", [1, "Product A", 100])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("order_items", [201, 101, 1])

        data = self.storage.db["DATA"]["order_items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0], [201, 101, 1])

    def test_insert_with_invalid_foreign_key(self):
        """Test inserting a row with invalid foreign key reference"""
        self.ddl_manager.create_table(
            "products_2",
            [("product_id", INT), ("name", STRING), ("price", INT)],
            primary_key="product_id",
        )
        self.ddl_manager.create_table(
            "order_items",
            [("order_item_id", INT), ("order_id", INT), ("product_id", INT)],
            primary_key="order_item_id",
            foreign_keys=[("product_id", "products_2", "product_id")],
        )
        self.dml_manager.insert("orders", [101, 1, 99.99])
        with self.assertRaises(ValueError):
            self.dml_manager.insert("order_items", [201, 101, 999])

    ########################## SELECT TESTS ##########################
    def test_select_all_columns(self):
        """Test selecting all columns"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        results = self.dml_manager.select("users")
        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0], {"id": 1, "name": "Alice", "email": "alice@example.com"}
        )

    def test_select_specific_columns(self):
        """Test selecting specific columns"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        results = self.dml_manager.select("users", columns=["id", "name"])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"id": 1, "name": "Alice"})

    def test_select_with_condition(self):
        """Test selecting with a where condition"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        # Select users with id > 1
        results = self.dml_manager.select("users", where=["id", ">", 1])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 2)

    def test_select_with_two_conditions_and(self):
        """Test selecting with where conditions"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("users", [3, "Charlie", "charlie@example.com"])

        # Select users with id > 1 and name = 'Bob'
        results = self.dml_manager.select(
            "users",
            where={"op": "AND", "left": ["id", ">", 1], "right": ["name", "=", "Bob"]},
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 2)
        self.assertEqual(results[0]["name"], "Bob")

    def test_select_with_two_conditions_or(self):
        """Test selecting with where conditions"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("users", [3, "Charlie", "charlie@example.com"])

        # Select users with id = 1 or name = 'Bob'
        results = self.dml_manager.select(
            "users",
            where={"op": "OR", "left": ["id", "=", 1], "right": ["name", "=", "Bob"]},
        )
        self.assertEqual(len(results), 2)
        ids = [result["id"] for result in results]
        names = [result["name"] for result in results]
        self.assertIn(1, ids)
        self.assertIn(2, ids)
        self.assertIn("Alice", names)
        self.assertIn("Bob", names)

    def test_select_with_group_by(self):
        """Test selecting with group by"""
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])

        results = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            group_by=["user_id"],
        )

        # print(results)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["user_id"], 1)
        self.assertEqual(
            results[0]["amount"], 99.99
        )  # First order amount with user_id 1
        self.assertEqual(results[1]["user_id"], 2)
        self.assertEqual(results[1]["amount"], 29.99)

    def test_select_with_aggregation(self):
        """Test selecting with aggregation"""
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])

        results = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            aggregates=[{MAX: "amount"}],
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["user_id"], 1)
        self.assertEqual(results[0]["amount"], 99.99)

    def test_select_with_group_by_and_aggregation(self):
        """Test selecting with group by and aggregation"""
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])

        results = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            group_by=["user_id"],
            aggregates=[{SUM: "amount"}],
        )

        # print(results)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["user_id"], 1)
        self.assertEqual(results[0]["amount"], (99.99 + 49.99))
        self.assertEqual(results[1]["user_id"], 2)
        self.assertEqual(results[1]["amount"], 29.99)

    def test_select_with_order_by(self):
        """Test selecting with order by"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])
        self.dml_manager.insert("orders", [104, 2, 199.99])

        result = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            order_by=[("user_id", DESC)],
        )
        expected = [
            {"user_id": 2, "amount": 29.99},
            {"user_id": 2, "amount": 199.99},
            {"user_id": 1, "amount": 99.99},
            {"user_id": 1, "amount": 49.99},
        ]

        self.assertEqual(result, expected)

        result = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            order_by=[("user_id", ASC), ("amount", DESC)],
        )
        expected = [
            {"user_id": 1, "amount": 99.99},
            {"user_id": 1, "amount": 49.99},
            {"user_id": 2, "amount": 199.99},
            {"user_id": 2, "amount": 29.99},
        ]

        self.assertEqual(result, expected)

    def test_select_with_aggregation_and_having(self):
        """Test selecting with group by and having"""
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 49.99])
        self.dml_manager.insert("orders", [104, 2, 29.99])
        self.dml_manager.insert("orders", [105, 3, 19.99])
        self.dml_manager.insert("orders", [106, 3, 29.99])

        results = self.dml_manager.select(
            "orders",
            columns=["user_id", "amount"],
            group_by=["user_id"],
            aggregates=[{SUM: "amount"}],
            having=lambda row: row["amount"] > 50,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["user_id"], 1)
        self.assertEqual(results[0]["amount"], (99.99 + 49.99))
        self.assertEqual(results[1]["user_id"], 2)
        self.assertEqual(results[1]["amount"], (49.99 + 29.99))

    ########################## DELETE TESTS ##########################
    def test_delete_all_rows(self):
        """Test deleting all rows from a table"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        deleted_count = self.dml_manager.delete("users")
        self.assertEqual(deleted_count, 2)
        self.assertEqual(len(self.storage.db["DATA"]["users"]), 0)

    def test_delete_with_condition(self):
        """Test deleting rows with a condition"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        # Delete user with id = 1
        deleted_count = self.dml_manager.delete(
            "users", where=lambda row: row["id"] == 1
        )
        self.assertEqual(deleted_count, 1)
        self.assertEqual(len(self.storage.db["DATA"]["users"]), 1)
        self.assertEqual(self.storage.db["DATA"]["users"][0][0], 2)

    def test_delete_with_two_conditions(self):
        """Test deleting rows with two conditions"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("users", [3, "Charlie", "charlie@example.com"])

        deleted_count = self.dml_manager.delete(
            "users",
            where={
                "op": "AND",
                "left": ["id", "<", 3],
                "right": ["name", "=", "Alice"],
            },
        )
        self.assertEqual(deleted_count, 1)
        self.assertEqual(len(self.storage.db["DATA"]["users"]), 2)
        self.assertEqual(self.storage.db["DATA"]["users"][0][0], 2)

        deleted_count = self.dml_manager.delete(
            "users",
            where={"op": "OR", "left": ["id", ">", 2], "right": ["name", "=", "Bob"]},
        )
        self.assertEqual(deleted_count, 2)
        self.assertEqual(len(self.storage.db["DATA"]["users"]), 0)

    def test_delete_updates_index(self):
        """Test that delete operation updates the index"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.delete("users")
        self.assertEqual(len(self.storage.index["users"]["id"]["tree"]), 0)

    ########################## UPDATE TESTS ##########################
    def test_update_all_rows(self):
        """Test updating all rows in a table"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        # Update all emails
        updated_count = self.dml_manager.update(
            "users", {"email": lambda x: x.replace("example.com", "test.org")}
        )
        self.assertEqual(updated_count, 2)
        self.assertEqual(self.storage.db["DATA"]["users"][0][2], "alice@test.org")
        self.assertEqual(self.storage.db["DATA"]["users"][1][2], "bob@test.org")

    def test_update_with_condition(self):
        """Test updating rows with a condition"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        # Update only Bob's email
        updated_count = self.dml_manager.update(
            "users", {"email": "new@example.com"}, where=lambda row: row["id"] == 2
        )
        self.assertEqual(updated_count, 1)
        self.assertEqual(self.storage.db["DATA"]["users"][0][2], "alice@example.com")
        self.assertEqual(self.storage.db["DATA"]["users"][1][2], "new@example.com")

    def test_update_with_two_conditions(self):
        """Test updating rows with a condition"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("users", [3, "Charlie", "charlie@example.com"])

        updated_count = self.dml_manager.update(
            "users",
            {"email": "new@example.com"},
            where={
                "op": "AND",
                "left": ["id", "<", 3],
                "right": ["name", "=", "Alice"],
            },
        )
        self.assertEqual(updated_count, 1)
        self.assertEqual(self.storage.db["DATA"]["users"][0][2], "new@example.com")
        self.assertEqual(self.storage.db["DATA"]["users"][1][2], "bob@example.com")

        updated_count = self.dml_manager.update(
            "users",
            {"email": "new2@example.com"},
            where={"op": "OR", "left": ["id", ">", 2], "right": ["name", "=", "Alice"]},
        )
        self.assertEqual(updated_count, 2)
        self.assertEqual(self.storage.db["DATA"]["users"][0][2], "new2@example.com")
        self.assertEqual(self.storage.db["DATA"]["users"][1][2], "bob@example.com")
        self.assertEqual(self.storage.db["DATA"]["users"][2][2], "new2@example.com")

    def test_update_updates_index(self):
        """Test that update operation updates the index"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.update("users", {"id": 10})
        self.assertNotIn(1, self.storage.index["users"]["id"]["tree"])
        self.assertIn(10, self.storage.index["users"]["id"]["tree"])

    def test_update_with_duplicate_primary_key(self):
        """Test updating a row with a duplicate primary key"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])

        with self.assertRaises(ValueError):
            self.dml_manager.update(
                "users", {"id": 1}, where=lambda row: row["id"] == 2
            )
        self.assertEqual(self.storage.db["DATA"]["users"][0][0], 1)
        self.assertEqual(self.storage.db["DATA"]["users"][1][0], 2)

    ########################## JOIN TESTS ##########################
    def test_select_join_with_index(self):
        """Test joining two tables using an index"""
        # Insert test data
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 2, 49.99])
        self.dml_manager.insert("orders", [103, 1, 29.99])

        # Perform join
        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
        )

        # Verify results
        self.assertEqual(len(results), 3)

        # Check that all joins are correct
        user_orders = {}
        for row in results:
            user_id = row["users.id"]
            if user_id not in user_orders:
                user_orders[user_id] = []
            user_orders[user_id].append(row["orders.order_id"])

        self.assertEqual(sorted(user_orders[1]), [101, 103])
        self.assertEqual(user_orders[2], [102])

    def test_select_join_with_specific_columns(self):
        """Test joining two tables with specific columns"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            columns=["users.name", "orders.amount"],
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {"users.name": "Alice", "orders.amount": 99.99})

    def test_select_join_with_condition(self):
        """Test joining two tables with additional condition"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 2, 49.99])

        # Join with condition: only orders with amount > 50
        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            where=lambda row: row["orders.amount"] > 50,
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["users.name"], "Alice")
        self.assertEqual(results[0]["orders.amount"], 99.99)

    def test_select_join_with_two_conditions(self):
        """Test joining two tables with additional conditions"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 49.99])

        # Join with condition: only orders with amount > 50
        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            where={
                "op": "AND",
                "left": ["orders.amount", ">", 50],
                "right": ["users.name", "=", "Alice"],
            },
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["users.name"], "Alice")
        self.assertEqual(results[0]["orders.amount"], 99.99)

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            where={
                "op": "OR",
                "left": ["orders.amount", ">", 50],
                "right": ["users.name", "=", "Alice"],
            },
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["users.name"], "Alice")
        self.assertEqual(results[0]["orders.amount"], 99.99)
        self.assertEqual(results[1]["users.name"], "Alice")
        self.assertEqual(results[1]["orders.amount"], 49.99)

    def test_select_join_with_self(self):
        """Test joining a table with itself"""
        # Insert test data
        self.ddl_manager.create_table(
            "employees",
            [("emp_id", INT), ("name", STRING), ("email", STRING), ("manager_id", INT)],
            primary_key="emp_id",
            foreign_keys=[("manager_id", "employees", "emp_id")],
        )
        self.dml_manager.insert("employees", [1, "Alice", "alice.example.com", None])
        self.dml_manager.insert("employees", [2, "Bob", "bob@example.com", 1])
        self.dml_manager.insert("employees", [3, "Charlie", "charlie@example.com", 2])
        self.dml_manager.insert("employees", [4, "David", "david@example.com", 2])

        # Perform join
        results = self.dml_manager.select_join_with_index(
            left_table="employees",  # employees_L = manager
            right_table="employees",  # employees_R = employee
            left_join_col="emp_id",
            right_join_col="manager_id",
            columns=["employees_L.name", "employees_R.name"],
        )

        self.assertEqual(len(results), 3)
        self.assertIn({"employees_L.name": "Alice", "employees_R.name": "Bob"}, results)
        self.assertIn(
            {"employees_L.name": "Bob", "employees_R.name": "Charlie"}, results
        )
        self.assertIn({"employees_L.name": "Bob", "employees_R.name": "David"}, results)

    def test_select_join_with_group_by(self):
        """Test join with group by"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])
        self.dml_manager.insert("orders", [104, 2, 199.99])

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            columns=["users.name", "orders.amount"],
            group_by=["users.name"],
        )

        expected = [
            {"users.name": "Alice", "orders.amount": 99.99},
            {"users.name": "Bob", "orders.amount": 29.99},
        ]

        self.assertEqual(len(results), 2)
        self.assertIn(expected[0], results)
        self.assertIn(expected[1], results)

    def test_select_join_with_group_by_and_aggregation(self):
        """Test join with group by and aggregation"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])
        self.dml_manager.insert("orders", [104, 2, 199.99])

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            columns=["users.name", "orders.amount"],
            group_by=["users.name"],
            aggregates=[{SUM: "orders.amount"}],
        )

        expected = [
            {"users.name": "Alice", "orders.amount": 149.98},
            {"users.name": "Bob", "orders.amount": 229.98},
        ]

        self.assertEqual(len(results), 2)
        self.assertIn(expected[0], results)
        self.assertIn(expected[1], results)

    def test_select_join_with_order_by(self):
        """Test join with ordering results"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])
        self.dml_manager.insert("orders", [104, 2, 199.99])

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            columns=["users.name", "orders.amount"],
            order_by=[("orders.amount", DESC)],
        )

        expected = [
            {"users.name": "Bob", "orders.amount": 199.99},
            {"users.name": "Alice", "orders.amount": 99.99},
            {"users.name": "Alice", "orders.amount": 49.99},
            {"users.name": "Bob", "orders.amount": 29.99},
        ]

        self.assertEqual(results, expected)

    def test_select_join_with_group_by_and_aggregation_and_having(self):
        """Test join with group by and aggregation and having"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.dml_manager.insert("orders", [101, 1, 99.99])
        self.dml_manager.insert("orders", [102, 1, 49.99])
        self.dml_manager.insert("orders", [103, 2, 29.99])
        self.dml_manager.insert("orders", [104, 2, 199.99])

        results = self.dml_manager.select_join_with_index(
            left_table="users",
            right_table="orders",
            left_join_col="id",
            right_join_col="user_id",
            columns=["users.name", "orders.amount"],
            group_by=["users.name"],
            aggregates=[{MAX: "orders.amount"}],
            having=lambda row: row["orders.amount"] > 150,
        )

        expected = [
            {"users.name": "Bob", "orders.amount": 199.99},
        ]

        self.assertEqual(len(results), 1)
        self.assertIn(expected[0], results)


if __name__ == "__main__":
    unittest.main()
