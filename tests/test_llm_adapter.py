"""Tests for LLM Adapter module."""
import pytest
from unittest.mock import patch, MagicMock
from src import llm_adapter


class TestBuildPrompt:
    def test_prompt_contains_schema(self):
        schema = "Tables:\n- sales (id, product, revenue)"
        system, user = llm_adapter.build_prompt(schema, "Show total revenue")
        assert "sales" in user
        assert "revenue" in user

    def test_prompt_contains_question(self):
        _, user = llm_adapter.build_prompt("schema", "How many products?")
        assert "How many products?" in user

    def test_system_prompt_mentions_select(self):
        system, _ = llm_adapter.build_prompt("schema", "question")
        assert "SELECT" in system


class TestExtractSQL:
    def test_plain_sql(self):
        assert llm_adapter.extract_sql("SELECT * FROM sales") == "SELECT * FROM sales"

    def test_with_markdown_fences(self):
        raw = "```sql\nSELECT * FROM sales\n```"
        assert llm_adapter.extract_sql(raw) == "SELECT * FROM sales"

    def test_with_whitespace(self):
        raw = "  \n  SELECT * FROM sales  \n  "
        assert llm_adapter.extract_sql(raw) == "SELECT * FROM sales"

    def test_with_generic_fence(self):
        raw = "```\nSELECT * FROM sales\n```"
        assert llm_adapter.extract_sql(raw) == "SELECT * FROM sales"


class TestGenerateSQL:
    @patch("src.llm_adapter.get_client")
    def test_calls_api(self, mock_get_client):
        # Mock the API response
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="SELECT COUNT(*) FROM sales")]
        mock_client.messages.create.return_value = mock_response

        result = llm_adapter.generate_sql("Tables: sales", "How many sales?")
        assert result == "SELECT COUNT(*) FROM sales"
        mock_client.messages.create.assert_called_once()

    @patch("src.llm_adapter.get_client")
    def test_strips_markdown(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="```sql\nSELECT * FROM sales\n```")]
        mock_client.messages.create.return_value = mock_response

        result = llm_adapter.generate_sql("schema", "show all")
        assert result == "SELECT * FROM sales"
