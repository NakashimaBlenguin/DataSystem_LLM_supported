"""
Data Loader: reads CSV files and inserts data into SQLite.
Uses pandas for reading CSV, but handles schema creation and insertion manually.
Does NOT use df.to_sql().
"""
import pandas as pd
import sqlite3
import os
from src import schema_manager


def validate_file(filepath):
    """Check that the file exists and is a CSV."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    if not filepath.lower().endswith(".csv"):
        raise ValueError(f"Not a CSV file: {filepath}")


def read_csv(filepath):
    """Read a CSV file into a pandas DataFrame."""
    validate_file(filepath)
    df = pd.read_csv(filepath)
    if df.empty:
        raise ValueError(f"CSV file is empty: {filepath}")
    return df


def insert_rows(conn, table_name, df, columns):
    """Insert all rows from DataFrame into the specified table."""
    col_names = [c[0] for c in columns]
    placeholders = ", ".join(["?"] * len(col_names))
    cols_str = ", ".join(col_names)
    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = []
        for orig_col, (norm_col, _) in zip(df.columns, columns):
            val = row[orig_col]
            # Convert NaN to None for SQLite
            if pd.isna(val):
                values.append(None)
            else:
                values.append(val)
        conn.execute(sql, values)
    conn.commit()


def load_csv_to_db(conn, filepath, table_name=None):
    """
    Main entry point: load a CSV file into the database.
    If table_name is None, derive it from the filename.
    Returns (table_name, row_count).
    """
    df = read_csv(filepath)
    columns = schema_manager.infer_column_types(df)

    # Try to find a matching existing table
    match = schema_manager.find_matching_table(conn, columns)

    if match:
        # Schema matches -> append data
        insert_rows(conn, match, df, columns)
        return match, len(df)

    # No match -> create new table
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(filepath))[0]
        table_name = schema_manager.normalize_column_name(table_name)

    schema_manager.create_table(conn, table_name, columns)
    insert_rows(conn, table_name, df, columns)
    return table_name, len(df)
