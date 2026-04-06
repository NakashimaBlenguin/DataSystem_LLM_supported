"""
CLI Interface: the user's only entry point to the system.
Does NOT directly access the database - all operations go through
query_service (for queries) or data_loader (for CSV ingestion).
"""
import sqlite3
import sys
import os
from src import data_loader, query_service, schema_manager


DEFAULT_DB = "database.db"


def get_connection(db_path=None):
    """Create a SQLite connection."""
    path = db_path or DEFAULT_DB
    return sqlite3.connect(path)


def print_help():
    """Print available commands."""
    print("\nAvailable commands:")
    print("  load <filepath>    - Load a CSV file into the database")
    print("  tables             - List all tables in the database")
    print("  schema <table>     - Show schema of a specific table")
    print("  sql <query>        - Execute a raw SQL SELECT query")
    print("  ask <question>     - Ask a natural language question (uses LLM)")
    print("  help               - Show this help message")
    print("  quit               - Exit the program")
    print()


def cmd_load(conn, filepath):
    """Handle the 'load' command."""
    try:
        table_name, count = data_loader.load_csv_to_db(conn, filepath)
        print(f"Loaded {count} rows into table '{table_name}'.")
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")


def cmd_tables(conn):
    """Handle the 'tables' command."""
    tables = schema_manager.get_existing_tables(conn)
    if not tables:
        print("No tables in the database.")
    else:
        print("Tables:")
        for t in tables:
            print(f"  - {t}")


def cmd_schema(conn, table_name):
    """Handle the 'schema' command."""
    tables = schema_manager.get_existing_tables(conn)
    if table_name not in tables:
        print(f"Table '{table_name}' not found.")
        return
    cols = schema_manager.get_table_schema(conn, table_name)
    print(f"Schema for '{table_name}':")
    print("  id INTEGER (PK)")
    for name, ctype in cols:
        print(f"  {name} {ctype}")


def cmd_sql(conn, sql):
    """Handle the 'sql' command (raw SQL, still validated)."""
    result = query_service.process_raw_sql_query(conn, sql)
    if result["sql"]:
        print(f"SQL: {result['sql']}")
    print(query_service.format_results(result))


def cmd_ask(conn, question):
    """Handle the 'ask' command (natural language -> LLM -> SQL)."""
    print("Generating SQL via LLM...")
    result = query_service.process_natural_language_query(conn, question)
    if result["sql"]:
        print(f"Generated SQL: {result['sql']}")
    print(query_service.format_results(result))


def main(db_path=None):
    """Main CLI loop."""
    conn = get_connection(db_path)
    print("=== Data Query System ===")
    print("Type 'help' for available commands.\n")

    while True:
        try:
            user_input = input(">> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        parts = user_input.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if command == "quit" or command == "exit":
            print("Goodbye!")
            break
        elif command == "help":
            print_help()
        elif command == "load":
            if not arg:
                print("Usage: load <filepath>")
            else:
                cmd_load(conn, arg)
        elif command == "tables":
            cmd_tables(conn)
        elif command == "schema":
            if not arg:
                print("Usage: schema <table_name>")
            else:
                cmd_schema(conn, arg)
        elif command == "sql":
            if not arg:
                print("Usage: sql <SELECT query>")
            else:
                cmd_sql(conn, arg)
        elif command == "ask":
            if not arg:
                print("Usage: ask <your question>")
            else:
                cmd_ask(conn, arg)
        else:
            print(f"Unknown command: {command}. Type 'help' for options.")

    conn.close()


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else None
    main(db)
