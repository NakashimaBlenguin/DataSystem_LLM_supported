"""Tests for SQL Validator module."""
import sqlite3
import pytest
from src import sql_validator
from src.sql_validator import ValidationError


@pytest.fixture
def conn():
    """Create an in-memory DB with test tables."""
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER, revenue REAL)")
    c.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, product_name TEXT, category TEXT, price REAL)")
    c.execute("INSERT INTO sales VALUES (1, 101, 5, 149.95)")
    c.execute("INSERT INTO products VALUES (1, 'Mouse', 'Electronics', 29.99)")
    c.commit()
    yield c
    c.close()


class TestSelectOnly:
    def test_valid_select(self, conn):
        sql = "SELECT * FROM sales"
        result = sql_validator.validate_query(conn, sql)
        assert result == sql

    def test_reject_insert(self, conn):
        with pytest.raises(ValidationError, match="INSERT"):
            sql_validator.validate_query(conn, "INSERT INTO sales VALUES (2, 102, 3, 89.97)")

    def test_reject_update(self, conn):
        with pytest.raises(ValidationError, match="UPDATE"):
            sql_validator.validate_query(conn, "UPDATE sales SET quantity = 10 WHERE id = 1")

    def test_reject_delete(self, conn):
        with pytest.raises(ValidationError, match="DELETE"):
            sql_validator.validate_query(conn, "DELETE FROM sales WHERE id = 1")

    def test_reject_drop(self, conn):
        with pytest.raises(ValidationError, match="DROP"):
            sql_validator.validate_query(conn, "DROP TABLE sales")

    def test_reject_alter(self, conn):
        with pytest.raises(ValidationError, match="ALTER"):
            sql_validator.validate_query(conn, "ALTER TABLE sales ADD COLUMN note TEXT")

    def test_reject_create(self, conn):
        with pytest.raises(ValidationError, match="CREATE"):
            sql_validator.validate_query(conn, "CREATE TABLE hack (id INTEGER)")


class TestDangerousPatterns:
    def test_reject_semicolon(self, conn):
        with pytest.raises(ValidationError, match="Multiple statements"):
            sql_validator.validate_query(conn, "SELECT * FROM sales; DROP TABLE sales")

    def test_reject_comment_dash(self, conn):
        with pytest.raises(ValidationError, match="comments"):
            sql_validator.validate_query(conn, "SELECT * FROM sales -- where id=1")

    def test_reject_comment_block(self, conn):
        with pytest.raises(ValidationError, match="comments"):
            sql_validator.validate_query(conn, "SELECT * FROM sales /* hack */")

    def test_reject_union(self, conn):
        with pytest.raises(ValidationError, match="UNION"):
            sql_validator.validate_query(conn, "SELECT * FROM sales UNION SELECT * FROM products")


class TestTableValidation:
    def test_valid_table(self, conn):
        sql_validator.validate_query(conn, "SELECT * FROM sales")

    def test_unknown_table(self, conn):
        with pytest.raises(ValidationError, match="does not exist"):
            sql_validator.validate_query(conn, "SELECT * FROM nonexistent")

    def test_join_valid_tables(self, conn):
        sql = "SELECT s.quantity, p.product_name FROM sales s JOIN products p ON s.product_id = p.id"
        sql_validator.validate_query(conn, sql)

    def test_join_unknown_table(self, conn):
        with pytest.raises(ValidationError):
            sql_validator.validate_query(conn, "SELECT * FROM sales JOIN fake ON sales.id = fake.id")


class TestColumnValidation:
    def test_valid_columns(self, conn):
        sql_validator.validate_query(conn, "SELECT quantity, revenue FROM sales")

    def test_invalid_column(self, conn):
        with pytest.raises(ValidationError, match="column"):
            sql_validator.validate_query(conn, "SELECT nonexistent_col FROM sales")

    def test_where_invalid_column(self, conn):
        with pytest.raises(ValidationError, match="column"):
            sql_validator.validate_query(conn, "SELECT * FROM sales WHERE fake_col = 1")


class TestEdgeCases:
    def test_empty_query(self, conn):
        with pytest.raises(ValidationError, match="Empty"):
            sql_validator.validate_query(conn, "")

    def test_whitespace_only(self, conn):
        with pytest.raises(ValidationError, match="Empty"):
            sql_validator.validate_query(conn, "   ")

    def test_case_insensitive_select(self, conn):
        sql_validator.validate_query(conn, "select * from sales")
        sql_validator.validate_query(conn, "SELECT * FROM sales")
        sql_validator.validate_query(conn, "Select * From sales")

    def test_query_with_limit(self, conn):
        sql_validator.validate_query(conn, "SELECT * FROM sales LIMIT 5")

    def test_query_with_order_by(self, conn):
        sql_validator.validate_query(conn, "SELECT * FROM sales ORDER BY revenue DESC")

    def test_query_with_aggregate(self, conn):
        sql_validator.validate_query(conn, "SELECT COUNT(*), SUM(revenue) FROM sales")

    def test_query_with_group_by(self, conn):
        sql_validator.validate_query(conn, "SELECT product_id, SUM(revenue) FROM sales GROUP BY product_id")

    def test_select_containing_write_keyword(self, conn):
        """Ensure write keywords inside a SELECT are still rejected."""
        with pytest.raises(ValidationError, match="DELETE"):
            sql_validator.validate_query(
                conn, "SELECT * FROM sales WHERE id IN (DELETE FROM products)"
            )

    def test_select_with_insert_keyword(self, conn):
        """Write keyword embedded in otherwise valid-looking query."""
        with pytest.raises(ValidationError, match="INSERT"):
            sql_validator.validate_query(
                conn, "SELECT * FROM sales WHERE quantity = (INSERT INTO sales VALUES (99,1,1,1))"
            )
