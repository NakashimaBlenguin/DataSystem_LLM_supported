"""Tests for Data Loader module."""
import sqlite3
import os
import pytest
import tempfile
from src import data_loader, schema_manager


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def sample_csv(tmp_path):
    """Create a temporary CSV file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("name,age,score\nAlice,30,95.5\nBob,25,88.0\n")
    return str(csv_file)


@pytest.fixture
def empty_csv(tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("name,age\n")
    return str(csv_file)


class TestValidateFile:
    def test_missing_file(self):
        with pytest.raises(FileNotFoundError):
            data_loader.validate_file("/nonexistent/file.csv")

    def test_not_csv(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("hello")
        with pytest.raises(ValueError, match="Not a CSV"):
            data_loader.validate_file(str(f))

    def test_valid_csv(self, sample_csv):
        data_loader.validate_file(sample_csv)  # Should not raise


class TestReadCSV:
    def test_read_valid(self, sample_csv):
        df = data_loader.read_csv(sample_csv)
        assert len(df) == 2
        assert "name" in df.columns

    def test_read_empty(self, empty_csv):
        with pytest.raises(ValueError, match="empty"):
            data_loader.read_csv(empty_csv)


class TestLoadCSVToDB:
    def test_load_creates_table(self, conn, sample_csv):
        table_name, count = data_loader.load_csv_to_db(conn, sample_csv)
        assert table_name == "test"
        assert count == 2
        tables = schema_manager.get_existing_tables(conn)
        assert "test" in tables

    def test_load_inserts_data(self, conn, sample_csv):
        data_loader.load_csv_to_db(conn, sample_csv)
        cursor = conn.execute("SELECT name, age, score FROM test")
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Alice"

    def test_load_with_custom_name(self, conn, sample_csv):
        table_name, _ = data_loader.load_csv_to_db(conn, sample_csv, table_name="my_data")
        assert table_name == "my_data"

    def test_append_on_matching_schema(self, conn, tmp_path):
        # Load first file
        csv1 = tmp_path / "data1.csv"
        csv1.write_text("x,y\n1,2\n3,4\n")
        data_loader.load_csv_to_db(conn, str(csv1), table_name="points")

        # Load second file with same schema
        csv2 = tmp_path / "data2.csv"
        csv2.write_text("x,y\n5,6\n")
        table_name, count = data_loader.load_csv_to_db(conn, str(csv2))

        # Should append to existing table
        assert table_name == "points"
        cursor = conn.execute("SELECT COUNT(*) FROM points")
        assert cursor.fetchone()[0] == 3

    def test_has_autoincrement_id(self, conn, sample_csv):
        data_loader.load_csv_to_db(conn, sample_csv)
        cursor = conn.execute("SELECT id FROM test")
        ids = [row[0] for row in cursor.fetchall()]
        assert ids == [1, 2]
