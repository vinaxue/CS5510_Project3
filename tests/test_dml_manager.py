import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager
from dml_manager import DMLManager


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
            [("id", "int"), ("name", "string"), ("email", "string")],
            primary_key="id",
        )
        self.ddl_manager.create_table(
            "orders",
            [("order_id", "int"), ("user_id", "int"), ("amount", "double")],
            primary_key="order_id",
        )
        self.ddl_manager.create_table(
            "products",
            [("product_id", "int"), ("name", "string"), ("price", "int")],
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
            self.dml_manager.insert(
                "users", ["not_an_int", "Alice", "alice@example.com"]
            )

    def test_insert_updates_index(self):
        """Test that insert operation updates the index"""
        self.dml_manager.insert("users", [2, "Bob", "bob@example.com"])
        self.assertIn(1, self.storage.index["users"]["id"])

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
        results = self.dml_manager.select("users", where=lambda row: row[0] > 1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 2)

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
        deleted_count = self.dml_manager.delete("users", where=lambda row: row[0] == 1)
        self.assertEqual(deleted_count, 1)
        self.assertEqual(len(self.storage.db["DATA"]["users"]), 1)
        self.assertEqual(self.storage.db["DATA"]["users"][0][0], 2)

    def test_delete_updates_index(self):
        """Test that delete operation updates the index"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.delete("users")
        self.assertEqual(len(self.storage.index["users"]["id"]), 0)

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
            "users", {"email": "new@example.com"}, where=lambda row: row[0] == 2
        )
        self.assertEqual(updated_count, 1)
        self.assertEqual(self.storage.db["DATA"]["users"][0][2], "alice@example.com")
        self.assertEqual(self.storage.db["DATA"]["users"][1][2], "new@example.com")

    def test_update_updates_index(self):
        """Test that update operation updates the index"""
        self.dml_manager.insert("users", [1, "Alice", "alice@example.com"])
        self.dml_manager.update("users", {"id": 10})
        self.assertNotIn(1, self.storage.index["users"]["id"])
        self.assertIn(10, self.storage.index["users"]["id"])

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


if __name__ == "__main__":
    unittest.main()
