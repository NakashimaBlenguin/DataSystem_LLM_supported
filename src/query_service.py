"""
Query Service: orchestrates the full query flow.
Flow: get schema -> send to LLM -> validate SQL -> execute -> format results.
This is the ONLY module that executes SQL queries against the database.
"""
import sqlite3
from src import schema_manager, sql_validator, llm_adapter


def process_natural_language_query(conn, user_question):
    """
    Full pipeline: natural language question -> validated SQL -> results.
    Returns dict with 'sql', 'columns', 'rows', and 'error' keys.
    """
    result = {"sql": None, "columns": [], "rows": [], "error": None}

    try:
        # Step 1: Get schema context
        schema_text = schema_manager.format_schema_for_prompt(conn)

        # Step 2: Generate SQL via LLM (untrusted)
        raw_sql = llm_adapter.generate_sql(schema_text, user_question)
        result["sql"] = raw_sql

        # Step 3: Validate the generated SQL
        validated_sql = sql_validator.validate_query(conn, raw_sql)

        # Step 4: Execute the validated query
        columns, rows = execute_query(conn, validated_sql)
        result["columns"] = columns
        result["rows"] = rows

    except sql_validator.ValidationError as e:
        result["error"] = f"Validation failed: {e}"
    except Exception as e:
        result["error"] = f"Error: {e}"

    return result


def process_raw_sql_query(conn, sql):
    """
    Execute a raw SQL query (still validated first).
    Used for manual SQL input mode.
    """
    result = {"sql": sql, "columns": [], "rows": [], "error": None}

    try:
        validated_sql = sql_validator.validate_query(conn, sql)
        columns, rows = execute_query(conn, validated_sql)
        result["columns"] = columns
        result["rows"] = rows
    except sql_validator.ValidationError as e:
        result["error"] = f"Validation failed: {e}"
    except Exception as e:
        result["error"] = f"Error: {e}"

    return result


def execute_query(conn, sql):
    """Execute a validated SELECT query and return (column_names, rows)."""
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return columns, rows


def format_results(result):
    """Format query results into a readable string."""
    if result["error"]:
        return f"Error: {result['error']}"

    if not result["rows"]:
        return "Query returned no results."

    # Build a simple text table
    columns = result["columns"]
    rows = result["rows"]

    # Calculate column widths
    widths = [len(str(c)) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    # Header
    header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(columns))
    separator = "-+-".join("-" * w for w in widths)

    # Rows
    lines = [header, separator]
    for row in rows:
        line = " | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row))
        lines.append(line)

    return "\n".join(lines)
