"""
LLM Adapter: sends schema + user question to Claude API, receives SQL.
The generated SQL is treated as UNTRUSTED input.
This module does NOT execute any SQL.
"""
import os
import re

try:
    import anthropic
except ImportError:
    anthropic = None


def get_client():
    """Create an Anthropic client. API key from ANTHROPIC_API_KEY env var."""
    if anthropic is None:
        raise ImportError("anthropic package not installed. Run: pip install anthropic")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable is not set.")
    return anthropic.Anthropic(api_key=api_key)


def build_prompt(schema_text, user_question):
    """Build the system and user prompts for SQL generation."""
    system_prompt = (
        "You are a SQL assistant. The database uses SQLite. "
        "Given the schema and a user question, generate ONLY a SELECT query. "
        "Output ONLY the SQL query, nothing else. No explanation, no markdown, no code blocks."
    )
    user_prompt = (
        f"Database schema:\n{schema_text}\n\n"
        f"User question: {user_question}\n\n"
        "Generate a single SELECT SQL query to answer this question."
    )
    return system_prompt, user_prompt


def extract_sql(response_text):
    """Extract SQL from the LLM response, stripping markdown fences if present."""
    text = response_text.strip()
    # Remove markdown code fences
    text = re.sub(r"```sql\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()
    return text


def generate_sql(schema_text, user_question):
    """
    Call Claude API to generate SQL from a natural language question.
    Returns the raw SQL string (untrusted, must be validated before execution).
    """
    client = get_client()
    system_prompt, user_prompt = build_prompt(schema_text, user_question)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw = message.content[0].text
    return extract_sql(raw)
