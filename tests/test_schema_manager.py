"""Tests for Schema Manager module."""
import sqlite3
import pytest
import pandas as pd
from src import schema_manager


@pytest.fixture
def conn():
    """Create an in-memory SQLite database for testing."""
    c = sqlite3.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def sample_df():
    """Create a sample DataFrame."""
    return pd.DataFrame({
        "Name": ["Alice", "Bob"],
        "Age": [30, 25],
        "Salary": [50000.0, 60000.0],
    })


class TestNormalizeColumnName:
    def test_basic(self):
        assert schema_manager.normalize_column_name("Name") == "name"

    def test_spaces(self):
        assert schema_manager.normalize_column_name("First Name") == "first_name"

    def test_special_chars(self):
        assert schema_manager.normalize_column_name("price ($)") == "price___"

    def test_leading_trailing_spaces(self):
        assert schema_manager.normalize_column_name("  age  ") == "age"


class TestInferColumnTypes:
    def test_basic_types(self, sample_df):
        cols = schema_manager.infer_column_types(sample_df)
        assert cols[0] == ("name", "TEXT")
        assert cols[1] == ("age", "INTEGER")
        assert cols[2] == ("salary", "REAL")

    def test_all_text(self):
        df = pd.DataFrame({"a": ["x"], "b": ["y"]})
        cols = schema_manager.infer_column_types(df)
        assert all(t == "TEXT" for _, t in cols)


class TestTableOperations:
    def test_create_and_list_tables(self, conn):
        cols = [("name", "TEXT"), ("age", "INTEGER")]
        schema_manager.create_table(conn, "people", cols)
        tables = schema_manager.get_existing_tables(conn)
        assert "people" in tables

    def test_get_table_schema(self, conn):
        cols = [("name", "TEXT"), ("age", "INTEGER")]
        schema_manager.create_table(conn, "people", cols)
        schema = schema_manager.get_table_schema(conn, "people")
        assert ("name", "TEXT") in schema
        assert ("age", "INTEGER") in schema

    def test_schema_excludes_id(self, conn):
        cols = [("name", "TEXT")]
        schema_manager.create_table(conn, "test", cols)
        schema = schema_manager.get_table_schema(conn, "test")
        col_names = [c[0] for c in schema]
        assert "id" not in col_names


class TestSchemaMatching:
    def test_matching_schemas(self):
        existing = [("name", "TEXT"), ("age", "INTEGER")]
        new = [("name", "TEXT"), ("age", "INTEGER")]
        assert schema_manager.schemas_match(existing, new) is True

    def test_different_column_count(self):
        existing = [("name", "TEXT")]
        new = [("name", "TEXT"), ("age", "INTEGER")]
        assert schema_manager.schemas_match(existing, new) is False

    def test_different_types(self):
        existing = [("name", "TEXT"), ("age", "TEXT")]
        new = [("name", "TEXT"), ("age", "INTEGER")]
        assert schema_manager.schemas_match(existing, new) is False

    def test_find_matching_table(self, conn):
        cols = [("name", "TEXT"), ("age", "INTEGER")]
        schema_manager.create_table(conn, "people", cols)
        match = schema_manager.find_matching_table(conn, cols)
        assert match == "people"

    def test_no_matching_table(self, conn):
        cols = [("x", "REAL")]
        match = schema_manager.find_matching_table(conn, cols)
        assert match is None


class TestBuildCreateTableSQL:
    def test_sql_has_id(self):
        sql = schema_manager.build_create_table_sql("t", [("a", "TEXT")])
        assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql

    def test_sql_has_columns(self):
        sql = schema_manager.build_create_table_sql("t", [("name", "TEXT"), ("val", "REAL")])
        assert "name TEXT" in sql
        assert "val REAL" in sql


class TestFormatSchema:
    def test_empty_db(self, conn):
        result = schema_manager.format_schema_for_prompt(conn)
        assert "no tables" in result.lower()

    def test_with_table(self, conn):
        schema_manager.create_table(conn, "items", [("name", "TEXT"), ("price", "REAL")])
        result = schema_manager.format_schema_for_prompt(conn)
        assert "items" in result
        assert "name" in result
