"""
Schema Manager: understands and manages the structure of the SQLite database.
Responsibilities: discover tables, infer types from CSV, compare schemas, provide schema info.
Does NOT execute data queries or call the LLM.
"""
import sqlite3
import re


# Maps Python/pandas types to SQLite types
TYPE_MAP = {
    "int64": "INTEGER",
    "float64": "REAL",
    "object": "TEXT",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
}


def infer_column_types(df):
    """Infer SQLite column types from a pandas DataFrame."""
    columns = []
    for col in df.columns:
        normalized = normalize_column_name(col)
        dtype = str(df[col].dtype)
        sql_type = TYPE_MAP.get(dtype, "TEXT")
        columns.append((normalized, sql_type))
    return columns


def normalize_column_name(name):
    """Normalize column name: lowercase, replace spaces/special chars with underscores."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def get_existing_tables(conn):
    """Return a list of all user-created table names in the database."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [row[0] for row in cursor.fetchall()]


def get_table_schema(conn, table_name):
    """Return list of (column_name, column_type) for a given table (excluding 'id' column)."""
    cursor = conn.execute(f"PRAGMA table_info('{table_name}')")
    rows = cursor.fetchall()
    # Each row: (cid, name, type, notnull, default_val, pk)
    return [(row[1], row[2]) for row in rows if row[1] != "id"]


def get_all_schemas(conn):
    """Return a dict of {table_name: [(col_name, col_type), ...]} for all tables."""
    tables = get_existing_tables(conn)
    schemas = {}
    for t in tables:
        schemas[t] = get_table_schema(conn, t)
    return schemas


def schemas_match(existing_schema, new_columns):
    """Check if a new set of columns matches an existing table schema exactly."""
    if len(existing_schema) != len(new_columns):
        return False
    for (e_name, e_type), (n_name, n_type) in zip(existing_schema, new_columns):
        if e_name != n_name or e_type != n_type:
            return False
    return True


def find_matching_table(conn, new_columns):
    """Find an existing table whose schema matches the given columns. Returns table name or None."""
    for table_name in get_existing_tables(conn):
        existing = get_table_schema(conn, table_name)
        if schemas_match(existing, new_columns):
            return table_name
    return None


def build_create_table_sql(table_name, columns):
    """Generate a CREATE TABLE statement with an auto-increment id primary key."""
    col_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
    for col_name, col_type in columns:
        col_defs.append(f"{col_name} {col_type}")
    cols_str = ", ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"


def create_table(conn, table_name, columns):
    """Create a new table in the database."""
    sql = build_create_table_sql(table_name, columns)
    conn.execute(sql)
    conn.commit()


def format_schema_for_prompt(conn):
    """Format all table schemas as a string for use in LLM prompts."""
    schemas = get_all_schemas(conn)
    if not schemas:
        return "The database has no tables."
    parts = []
    for table_name, columns in schemas.items():
        cols_str = ", ".join(f"{name} ({ctype})" for name, ctype in columns)
        parts.append(f"- {table_name} (id INTEGER PK, {cols_str})")
    return "Tables:\n" + "\n".join(parts)
