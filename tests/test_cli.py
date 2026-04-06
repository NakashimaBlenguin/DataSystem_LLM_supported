"""Tests for CLI module."""
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from src import cli, schema_manager


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
    c.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")
    c.commit()
    yield c
    c.close()


class TestCmdTables:
    def test_lists_tables(self, conn, capsys):
        cli.cmd_tables(conn)
        output = capsys.readouterr().out
        assert "items" in output

    def test_empty_db(self, capsys):
        c = sqlite3.connect(":memory:")
        cli.cmd_tables(c)
        output = capsys.readouterr().out
        assert "No tables" in output
        c.close()


class TestCmdSchema:
    def test_shows_columns(self, conn, capsys):
        cli.cmd_schema(conn, "items")
        output = capsys.readouterr().out
        assert "name" in output
        assert "TEXT" in output

    def test_unknown_table(self, conn, capsys):
        cli.cmd_schema(conn, "nonexistent")
        output = capsys.readouterr().out
        assert "not found" in output


class TestCmdLoad:
    def test_load_valid_csv(self, conn, tmp_path, capsys):
        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n3,4\n")
        cli.cmd_load(conn, str(csv))
        output = capsys.readouterr().out
        assert "Loaded 2 rows" in output

    def test_load_missing_file(self, conn, capsys):
        cli.cmd_load(conn, "/nonexistent/file.csv")
        output = capsys.readouterr().out
        assert "Error" in output


class TestCmdSql:
    def test_valid_query(self, conn, capsys):
        cli.cmd_sql(conn, "SELECT name FROM items")
        output = capsys.readouterr().out
        assert "Widget" in output

    def test_invalid_query(self, conn, capsys):
        cli.cmd_sql(conn, "DROP TABLE items")
        output = capsys.readouterr().out
        assert "Validation failed" in output
