import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager
from BTrees.OOBTree import OOBTree


class TestDDLManager(unittest.TestCase):
    def setUp(self):
        """Set up a test environment with a temporary database"""
        self.db_file = "./test_database.dat"
        self.index_file = "./test_index.db"
        self.storage = StorageManager(self.db_file, self.index_file)
        self.ddl_manager = DDLManager(self.storage)

    def tearDown(self):
        """Clean up test files"""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.index_file):
            os.remove(self.index_file)

    ########################## CREATE TABLE ##########################
    def test_create_table(self):
        """Test creating a new table and check if primary key index is created"""
        # Create table with primary key and column types
        self.ddl_manager.create_table(
            "users",
            [("id", "int"), ("name", "str"), ("email", "str")],
            primary_key="id",
        )

        db = self.storage.load_db()
        self.assertIn("users", db["TABLES"])
        self.assertEqual(db["TABLES"]["users"]["primary_key"], "id")
        self.assertEqual(
            db["COLUMNS"]["users"], [("id", "int"), ("name", "str"), ("email", "str")]
        )

        # Check if the index exists for the primary key column
        self.assertIn("users", self.storage.index)
        self.assertIn(
            "id", self.storage.index["users"]
        )  # Index should be created on primary key
        self.assertNotIn(
            "name", self.storage.index["users"]
        )  # No index should be created on non-primary key columns
        self.assertNotIn("email", self.storage.index["users"])

    def test_create_duplicate_table(self):
        """Test creating a duplicate table raises an error"""
        self.ddl_manager.create_table(
            "users", [("id", "int"), ("name", "str")], primary_key="id"
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.create_table(
                "users", [("id", "int"), ("name", "str")], primary_key="id"
            )

    def test_create_table_with_foreign_keys(self):
        """Test creating a table with foreign keys"""
        self.ddl_manager.create_table(
            "departments", [("id", "int"), ("name", "str")], primary_key="id"
        )
        self.ddl_manager.create_table(
            "employees",
            [("id", "int"), ("name", "str"), ("dept_id", "int")],
            primary_key="id",
            foreign_keys=[("dept_id", "departments", "id")],
        )
        db = self.storage.load_db()
        self.assertIn("employees", db["TABLES"])
        self.assertIn("FOREIGN_KEYS", db)
        self.assertIsInstance(db["FOREIGN_KEYS"], dict)
        self.assertIn("employees", db["FOREIGN_KEYS"])
        self.assertEqual(
            db["FOREIGN_KEYS"]["employees"],
            {"dept_id": {"referenced_table": "departments", "referenced_column": "id"}},
        )

        # Check that primary key index exists for "employees" table
        self.assertIn("employees", self.storage.index)
        self.assertIn("id", self.storage.index["employees"])  # Index on primary key

    ########################## DROP TABLE ##########################
    def test_drop_table(self):
        """Test dropping an existing table"""
        self.ddl_manager.create_table(
            "users", [("id", "int"), ("name", "str")], primary_key="id"
        )
        self.ddl_manager.drop_table("users")
        db = self.storage.load_db()
        self.assertNotIn("users", db["TABLES"])
        self.assertNotIn("users", db["COLUMNS"])
        self.assertNotIn("users", db["DATA"])
        self.assertNotIn("users", self.storage.index)

    def test_drop_nonexistent_table(self):
        """Test dropping a table that does not exist raises an error"""
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_table("nonexistent_table")

    def test_drop_table_with_foreign_key_dependency(self):
        """Test dropping a table that is referenced by a foreign key raises an error"""
        self.ddl_manager.create_table(
            "departments", [("id", "int"), ("name", "str")], primary_key="id"
        )
        self.ddl_manager.create_table(
            "employees",
            [("id", "int"), ("name", "str"), ("dept_id", "int")],
            primary_key="id",
            foreign_keys=[("dept_id", "departments", "id")],
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_table("departments")

    ########################## CREATE INDEX ##########################
    def test_create_index(self):
        """Test creating an index on an existing column"""
        # Create table and insert data
        self.ddl_manager.create_table(
            "users",
            [("id", "int"), ("name", "str"), ("email", "str")],
            primary_key="id",
        )

        # Directly adding data to the database
        # TODO: replace once insert is implemented
        self.storage.db["DATA"]["users"] = [
            [1, "Alice", "alice@example.com"],
            [2, "Bob", "bob@example.com"],
        ]
        self.storage.db["COLUMNS"]["users"] = [
            ("id", "int"),
            ("name", "str"),
            ("email", "str"),
        ]

        # Manually update the index for primary key
        self.storage.index["users"]["id"] = OOBTree()
        self.storage.index["users"]["id"][1] = [0]
        self.storage.index["users"]["id"][2] = [1]

        # Save the changes to the database and index
        self.storage.save_db()
        self.storage.save_index()

        # Create an index on the "email" column
        self.ddl_manager.create_index("users", "email")

        # Check if the index exists in the storage
        db = self.storage.load_db()
        self.assertIn("users", db["TABLES"])
        self.assertIn("email", self.storage.index["users"])

        # Check that the index is populated correctly
        index = self.storage.index["users"]["email"]
        self.assertIn("alice@example.com", index)
        self.assertIn("bob@example.com", index)

    def test_create_index_on_nonexistent_table(self):
        """Test creating an index on a table that does not exist"""
        with self.assertRaises(ValueError):
            self.ddl_manager.create_index("nonexistent_table", "email")

    def test_create_index_on_nonexistent_column(self):
        """Test creating an index on a column that does not exist"""
        self.ddl_manager.create_table(
            "users", [("id", "int"), ("name", "str")], primary_key="id"
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.create_index("users", "email")

    ########################## DROP INDEX ##########################
    def test_drop_index(self):
        """Test dropping an existing index"""
        # Create table and insert data
        self.ddl_manager.create_table(
            "users",
            [("id", "int"), ("name", "str"), ("email", "str")],
            primary_key="id",
        )

        # Directly adding data to the database
        # TODO: replace once insert is implemented
        self.storage.db["DATA"]["users"] = [
            [1, "Alice", "alice@example.com"],
            [2, "Bob", "bob@example.com"],
        ]
        self.storage.db["COLUMNS"]["users"] = [
            ("id", "int"),
            ("name", "str"),
            ("email", "str"),
        ]

        # Manually update the index for primary key
        self.storage.index["users"]["id"] = OOBTree()
        self.storage.index["users"]["id"][1] = [0]
        self.storage.index["users"]["id"][2] = [1]

        # Save the changes to the database and index
        self.storage.save_db()
        self.storage.save_index()

        # Create an index on the "email" column
        self.ddl_manager.create_index("users", "email")
        self.assertIn("email", self.storage.index["users"])

        # Drop the index
        self.ddl_manager.drop_index("users", "email")
        self.assertNotIn("email", self.storage.index["users"])

    def test_drop_nonexistent_index(self):
        """Test dropping an index that does not exist"""
        self.ddl_manager.create_table(
            "users", [("id", "int"), ("name", "str")], primary_key="id"
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_index("users", "email")

    def test_drop_index_on_nonexistent_table(self):
        """Test dropping an index on a table that does not exist"""
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_index("nonexistent_table", "email")

    def test_drop_index_on_nonexistent_column(self):
        """Test dropping an index on a column that does not exist"""
        self.ddl_manager.create_table(
            "users", [("id", "int"), ("name", "str")], primary_key="id"
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_index("users", "email")


if __name__ == "__main__":
    unittest.main()
