import os
import unittest
from storage_manager import StorageManager
from ddl_manager import DDLManager


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
        """Test creating a new table"""
        self.ddl_manager.create_table(
            "users", ["id", "name", "email"], primary_key="id"
        )

        db = self.storage.load_db()
        self.assertIn("users", db["TABLES"])
        self.assertEqual(db["TABLES"]["users"]["primary_key"], "id")
        self.assertEqual(db["COLUMNS"]["users"], ["id", "name", "email"])
        self.assertIn("users", self.storage.index)
        self.assertIn("id", self.storage.index["users"])
        self.assertIn("name", self.storage.index["users"])
        self.assertIn("email", self.storage.index["users"])

    def test_create_duplicate_table(self):
        """Test creating a duplicate table raises an error"""
        self.ddl_manager.create_table("users", ["id", "name"], primary_key="id")
        with self.assertRaises(ValueError):
            self.ddl_manager.create_table("users", ["id", "name"], primary_key="id")

    def test_create_table_with_foreign_keys(self):
        """Test creating a table with foreign keys"""
        self.ddl_manager.create_table("departments", ["id", "name"], primary_key="id")
        self.ddl_manager.create_table(
            "employees",
            ["id", "name", "dept_id"],
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

    ########################## DROP TABLE ##########################
    def test_drop_table(self):
        """Test dropping an existing table"""
        self.ddl_manager.create_table("users", ["id", "name"], primary_key="id")
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
        self.ddl_manager.create_table("departments", ["id", "name"], primary_key="id")
        self.ddl_manager.create_table(
            "employees",
            ["id", "name", "dept_id"],
            primary_key="id",
            foreign_keys=[("dept_id", "departments", "id")],
        )
        with self.assertRaises(ValueError):
            self.ddl_manager.drop_table("departments")


if __name__ == "__main__":
    unittest.main()
