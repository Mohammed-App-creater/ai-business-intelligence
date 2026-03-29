"""
Tests for prompt changes — documents field in RagChatData
and rendering in rag_chat prompts.
"""

from __future__ import annotations

import pytest

from app.prompts.types import RagChatData, RevenueEntry
from app.prompts.rag_chat.anthropic import build as anthropic_build
from app.prompts.rag_chat.openai import build as openai_build


# ═══════════════════════════════════════════════════════════════════════════
# RagChatData.documents field
# ═══════════════════════════════════════════════════════════════════════════

class TestRagChatDataDocuments:

    def test_documents_defaults_empty(self):
        data = RagChatData(
            business_id="42",
            business_type="Salon",
            analysis_period="March 2026",
            question="Why did revenue drop?",
        )
        assert data.documents == []

    def test_documents_can_be_populated(self):
        data = RagChatData(
            business_id="42",
            business_type="Salon",
            analysis_period="March 2026",
            question="test",
            documents=["doc 1 text", "doc 2 text"],
        )
        assert len(data.documents) == 2

    def test_documents_and_typed_entries_coexist(self):
        data = RagChatData(
            business_id="42",
            business_type="Salon",
            analysis_period="March 2026",
            question="test",
            documents=["some context"],
            revenue=[RevenueEntry(period="March", amount=9200)],
        )
        assert len(data.documents) == 1
        assert len(data.revenue) == 1


# ═══════════════════════════════════════════════════════════════════════════
# Anthropic prompt — document rendering
# ═══════════════════════════════════════════════════════════════════════════

class TestAnthropicDocuments:

    def _data_with_docs(self, docs: list[str]) -> RagChatData:
        return RagChatData(
            business_id="42",
            business_type="Hair Salon",
            analysis_period="March 2026",
            question="Why did revenue drop?",
            documents=docs,
        )

    def test_documents_rendered_in_xml_tags(self):
        data = self._data_with_docs(["Revenue was $9,200 in March."])
        _, user = anthropic_build(data)
        assert "<retrieved_context>" in user
        assert "</retrieved_context>" in user
        assert '<document index="1">' in user
        assert "Revenue was $9,200 in March." in user

    def test_multiple_documents_numbered(self):
        data = self._data_with_docs(["Doc one.", "Doc two.", "Doc three."])
        _, user = anthropic_build(data)
        assert '<document index="1">' in user
        assert '<document index="2">' in user
        assert '<document index="3">' in user

    def test_no_documents_no_section(self):
        data = self._data_with_docs([])
        _, user = anthropic_build(data)
        assert "<retrieved_context>" not in user

    def test_documents_inside_business_data(self):
        data = self._data_with_docs(["Some context."])
        _, user = anthropic_build(data)
        assert "<business_data>" in user
        assert "<retrieved_context>" in user

    def test_documents_plus_typed_entries_both_rendered(self):
        data = RagChatData(
            business_id="42",
            business_type="Salon",
            analysis_period="March 2026",
            question="test",
            documents=["Retrieved doc text."],
            revenue=[RevenueEntry(period="March", amount=9200)],
        )
        _, user = anthropic_build(data)
        assert "<retrieved_context>" in user
        assert "<revenue>" in user

    def test_system_prompt_unchanged(self):
        data = self._data_with_docs(["test"])
        system, _ = anthropic_build(data)
        assert "<role>" in system
        assert "<rules>" in system


# ═══════════════════════════════════════════════════════════════════════════
# OpenAI prompt — document rendering
# ═══════════════════════════════════════════════════════════════════════════

class TestOpenAIDocuments:

    def _data_with_docs(self, docs: list[str]) -> RagChatData:
        return RagChatData(
            business_id="42",
            business_type="Hair Salon",
            analysis_period="March 2026",
            question="Why did revenue drop?",
            documents=docs,
        )

    def test_documents_rendered_with_headers(self):
        data = self._data_with_docs(["Revenue was $9,200 in March."])
        _, user = openai_build(data)
        assert "## Retrieved Context" in user
        assert "### Document 1" in user
        assert "Revenue was $9,200 in March." in user

    def test_multiple_documents_numbered(self):
        data = self._data_with_docs(["One.", "Two.", "Three."])
        _, user = openai_build(data)
        assert "### Document 1" in user
        assert "### Document 2" in user
        assert "### Document 3" in user

    def test_no_documents_no_section(self):
        data = self._data_with_docs([])
        _, user = openai_build(data)
        assert "## Retrieved Context" not in user

    def test_documents_plus_typed_entries_both_rendered(self):
        data = RagChatData(
            business_id="42",
            business_type="Salon",
            analysis_period="March 2026",
            question="test",
            documents=["Retrieved doc."],
            revenue=[RevenueEntry(period="March", amount=9200)],
        )
        _, user = openai_build(data)
        assert "## Retrieved Context" in user
        assert "## Revenue" in user

    def test_system_prompt_unchanged(self):
        data = self._data_with_docs(["test"])
        system, _ = openai_build(data)
        assert "expert business analytics assistant" in system