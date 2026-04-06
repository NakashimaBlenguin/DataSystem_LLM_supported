# Data Query System with LLM Interface

A modular Python system that loads structured CSV data into SQLite and allows users to query data via natural language, using an LLM (Claude) to translate questions into SQL.

## System Overview

### Architecture

The system follows a strict separation of concerns with two independent data flows:

**Ingestion flow:** CLI → Data Loader → Schema Manager → SQLite  
**Query flow:** CLI → Query Service → LLM Adapter → SQL Validator → SQLite

Key design constraints:
- The **CLI does not access the database directly** — all operations go through the Query Service or Data Loader
- The **LLM Adapter only generates SQL** — it never executes queries
- All LLM-generated SQL is treated as **untrusted input** and must pass through the SQL Validator before execution

### Modules

| Module | Responsibility |
|--------|---------------|
| `cli.py` | Command-line interface, user entry point |
| `data_loader.py` | Reads CSV files, inserts data into SQLite |
| `schema_manager.py` | Manages table schemas, infers types, detects matches |
| `query_service.py` | Orchestrates query flow, executes validated SQL |
| `llm_adapter.py` | Calls Claude API to translate natural language → SQL |
| `sql_validator.py` | Validates SQL safety (SELECT only, checks tables/columns) |

## How to Run

### Prerequisites
- Python 3.9+
- An Anthropic API key (for LLM features)

### Setup

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="your-api-key-here"
```

### Running the CLI

```bash
python -m src.cli
```

### CLI Commands

```
load <filepath>    - Load a CSV file into the database
tables             - List all tables
schema <table>     - Show table schema
sql <query>        - Run a raw SQL SELECT query (validated)
ask <question>     - Ask a natural language question (uses LLM)
help               - Show help
quit               - Exit
```

### Example Session

```
>> load data/products.csv
Loaded 5 rows into table 'products'.

>> load data/sales.csv
Loaded 10 rows into table 'sales'.

>> tables
Tables:
  - products
  - sales

>> sql SELECT product_name, price FROM products WHERE price > 100
product_name     | price
-----------------+------
Mechanical Keyboard | 299.99
Monitor Stand       | 299.99

>> ask What are the top 3 products by total revenue?
Generated SQL: SELECT p.product_name, SUM(s.revenue) as total ...
```

## How to Run Tests

```bash
pytest tests/ -v
```

All tests use mocked data and in-memory SQLite databases. The LLM adapter tests mock the API calls so no API key is needed for testing.

## Design Decisions

### Why CLI cannot access the database directly
This enforces a clean separation of concerns. The CLI is purely a user interface — it delegates all data operations to specialized modules. This makes the system easier to test and allows swapping the CLI for a web interface without changing any business logic.

### Why LLM output is untrusted
LLMs can hallucinate table names, generate write queries when asked read-only questions, or produce syntactically invalid SQL. By treating all LLM output as untrusted input and validating it through the SQL Validator, the system remains correct even when the LLM is wrong.

### SQL Validator design
The validator operates at the query structure level:
1. Checks query starts with SELECT (rejects all write operations)
2. Rejects dangerous patterns (semicolons, comments, UNION)
3. Verifies all referenced tables exist
4. Uses SQLite's EXPLAIN to verify column references
This approach is lightweight but catches the most common failure modes.

### Schema matching for CSV ingestion
When loading a CSV, the Schema Manager checks if any existing table has a matching schema (same column names and types). If so, data is appended rather than creating a duplicate table. This prevents accidental table proliferation.

## AI Usage Documentation

### Runtime LLM Integration
The LLM Adapter calls the Claude API at runtime to translate natural language queries into SQL. This is the intended use of AI as a system component, not a development aid.

### Development Process
The overall system architecture (module boundaries, data flows, API definitions) was designed by me. During implementation, I used Claude as a development companion to help me understand and work through specific technical details I was unfamiliar with. The general workflow was: I would describe what a module needed to do, and the AI would explain approaches or help me figure out how to implement specific functions (e.g., how to use `PRAGMA table_info()` to inspect schemas, how to safely extract table names from SQL with regex, how to structure the prompt for SQL generation). I then wrote the code myself based on that guidance.

Specifically, AI was used as a learning and implementation aid for the following modules:
- **Schema Manager**: AI helped me understand SQLite's `PRAGMA` commands and how to map pandas dtypes to SQLite types. The schema comparison and normalization logic was implemented with AI guidance.
- **Data Loader**: AI assisted with the manual row insertion approach (since `df.to_sql()` was not allowed) and handling of NaN-to-None conversion.
- **SQL Validator**: AI helped me explore strategies for query-level validation (checking for write keywords, dangerous patterns, using `EXPLAIN` for column verification). I defined the API and validation rules; AI helped with the regex patterns and the implementation of each check function.
- **LLM Adapter**: AI helped me structure the prompt engineering and the response extraction (stripping markdown fences from LLM output).
- **Query Service**: AI guided me on how to orchestrate the modules together and format the text-based result table.
- **CLI**: AI helped with the command parsing loop and dispatching structure.

### Unit Tests
All pytest unit tests were designed and written by me. The test cases, assertions, and mocking strategies reflect my understanding of what each module should and should not do. AI was not used for test generation.
