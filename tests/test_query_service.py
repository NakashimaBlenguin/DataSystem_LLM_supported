"""Tests for Query Service module."""
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from src import query_service, schema_manager


@pytest.fixture
def conn():
    """DB with sample data."""
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, revenue REAL)")
    c.execute("INSERT INTO sales VALUES (1, 'Mouse', 5, 149.95)")
    c.execute("INSERT INTO sales VALUES (2, 'Keyboard', 2, 599.98)")
    c.execute("INSERT INTO sales VALUES (3, 'Cable', 10, 49.90)")
    c.commit()
    yield c
    c.close()


class TestExecuteQuery:
    def test_basic_select(self, conn):
        cols, rows = query_service.execute_query(conn, "SELECT * FROM sales")
        assert "product" in cols
        assert len(rows) == 3

    def test_select_with_where(self, conn):
        cols, rows = query_service.execute_query(conn, "SELECT product FROM sales WHERE quantity > 3")
        assert len(rows) == 2


class TestProcessRawSQL:
    def test_valid_query(self, conn):
        result = query_service.process_raw_sql_query(conn, "SELECT * FROM sales")
        assert result["error"] is None
        assert len(result["rows"]) == 3

    def test_invalid_query_write(self, conn):
        result = query_service.process_raw_sql_query(conn, "DELETE FROM sales")
        assert result["error"] is not None
        assert "Validation failed" in result["error"]

    def test_invalid_table(self, conn):
        result = query_service.process_raw_sql_query(conn, "SELECT * FROM nonexistent")
        assert result["error"] is not None


class TestProcessNaturalLanguage:
    @patch("src.llm_adapter.generate_sql")
    def test_valid_llm_response(self, mock_llm, conn):
        mock_llm.return_value = "SELECT product, revenue FROM sales ORDER BY revenue DESC"
        result = query_service.process_natural_language_query(conn, "What are the top products by revenue?")
        assert result["error"] is None
        assert len(result["rows"]) == 3
        assert result["rows"][0][0] == "Keyboard"

    @patch("src.llm_adapter.generate_sql")
    def test_llm_returns_invalid_sql(self, mock_llm, conn):
        """LLM generates a DELETE - validator should catch it."""
        mock_llm.return_value = "DELETE FROM sales WHERE id = 1"
        result = query_service.process_natural_language_query(conn, "Delete all sales")
        assert result["error"] is not None
        assert "Validation failed" in result["error"]

    @patch("src.llm_adapter.generate_sql")
    def test_llm_returns_nonexistent_table(self, mock_llm, conn):
        """LLM hallucinates a table name - validator catches it."""
        mock_llm.return_value = "SELECT * FROM orders"
        result = query_service.process_natural_language_query(conn, "Show me all orders")
        assert result["error"] is not None

    @patch("src.llm_adapter.generate_sql")
    def test_llm_returns_nonexistent_column(self, mock_llm, conn):
        """LLM hallucinates a column name - validator catches it."""
        mock_llm.return_value = "SELECT fake_column FROM sales"
        result = query_service.process_natural_language_query(conn, "Show fake data")
        assert result["error"] is not None


class TestFormatResults:
    def test_format_with_data(self):
        result = {"error": None, "columns": ["name", "val"], "rows": [("a", 1), ("b", 2)], "sql": ""}
        formatted = query_service.format_results(result)
        assert "name" in formatted
        assert "a" in formatted

    def test_format_error(self):
        result = {"error": "Something went wrong", "columns": [], "rows": [], "sql": ""}
        formatted = query_service.format_results(result)
        assert "Error" in formatted

    def test_format_empty(self):
        result = {"error": None, "columns": ["x"], "rows": [], "sql": ""}
        formatted = query_service.format_results(result)
        assert "no results" in formatted.lower()
