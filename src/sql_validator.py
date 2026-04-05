"""
SQL Validator: validates SQL queries for safety before execution.
Rules:
  - Only SELECT queries allowed
  - Referenced tables must exist in the database
  - Referenced columns must exist in the referenced tables
  - No dangerous patterns (comments, semicolons for injection, etc.)
"""
import re
import sqlite3


class ValidationError(Exception):
    """Raised when SQL validation fails."""
    pass


def validate_query(conn, sql):
    """
    Validate a SQL query against the database schema.
    Raises ValidationError if the query is not safe to execute.
    Returns the cleaned SQL string if valid.
    """
    sql = sql.strip()

    _check_not_empty(sql)
    _check_select_only(sql)
    _check_dangerous_patterns(sql)
    tables = _extract_tables(sql)
    _check_tables_exist(conn, tables)
    _check_columns_exist(conn, sql, tables)

    return sql


def _check_not_empty(sql):
    if not sql:
        raise ValidationError("Empty query.")


def _check_select_only(sql):
    """Ensure query starts with SELECT (case-insensitive)."""
    first_word = sql.split()[0].upper() if sql.split() else ""
    if first_word != "SELECT":
        raise ValidationError(
            f"Only SELECT queries are allowed. Got: {first_word}"
        )

    # Check for write keywords anywhere in the query
    write_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE", "TRUNCATE"]
    upper_sql = sql.upper()
    for kw in write_keywords:
        # Match as whole word to avoid false positives
        if re.search(rf"\b{kw}\b", upper_sql):
            raise ValidationError(f"Write operation '{kw}' is not allowed.")


def _check_dangerous_patterns(sql):
    """Reject patterns commonly used in SQL injection."""
    # Multiple statements (semicolons)
    if ";" in sql:
        raise ValidationError("Multiple statements (;) are not allowed.")

    # SQL comments
    if "--" in sql or "/*" in sql:
        raise ValidationError("SQL comments are not allowed.")

    # UNION-based injection attempts
    if re.search(r"\bUNION\b", sql, re.IGNORECASE):
        raise ValidationError("UNION queries are not allowed.")


def _extract_tables(sql):
    """Extract table names referenced in FROM and JOIN clauses."""
    tables = set()

    # Match FROM table_name and JOIN table_name
    from_pattern = r"\bFROM\s+(\w+)"
    join_pattern = r"\bJOIN\s+(\w+)"

    for match in re.finditer(from_pattern, sql, re.IGNORECASE):
        tables.add(match.group(1).lower())
    for match in re.finditer(join_pattern, sql, re.IGNORECASE):
        tables.add(match.group(1).lower())

    if not tables:
        raise ValidationError("No table found in query.")

    return tables


def _get_db_tables(conn):
    """Get all table names in the database."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {row[0].lower() for row in cursor.fetchall()}


def _get_table_columns(conn, table_name):
    """Get all column names for a given table."""
    cursor = conn.execute(f"PRAGMA table_info('{table_name}')")
    return {row[1].lower() for row in cursor.fetchall()}


def _check_tables_exist(conn, tables):
    """Verify all referenced tables exist in the database."""
    db_tables = _get_db_tables(conn)
    for t in tables:
        if t.lower() not in db_tables:
            raise ValidationError(
                f"Table '{t}' does not exist. Available tables: {', '.join(sorted(db_tables))}"
            )


def _check_columns_exist(conn, sql, tables):
    """
    Verify that column references in the query exist in the referenced tables.
    Uses SQLite's own parser via EXPLAIN to catch invalid columns.
    """
    try:
        conn.execute(f"EXPLAIN {sql}")
    except sqlite3.OperationalError as e:
        msg = str(e)
        if "no such column" in msg:
            raise ValidationError(f"Invalid column reference: {msg}")
        if "no such table" in msg:
            raise ValidationError(f"Invalid table reference: {msg}")
        raise ValidationError(f"SQL error: {msg}")
