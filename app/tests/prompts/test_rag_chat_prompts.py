"""
test_rag_chat_prompts.py
========================
Tests for rag_chat prompts — both providers.
"""
import pytest
from app.prompts import build_prompt
from app.prompts.types import (
    RagChatData, RevenueEntry, AppointmentEntry,
    StaffEntry, ServiceEntry, CampaignEntry,
)
from app.services.llm.types import UseCase, Provider


def _build(provider, data):
    return build_prompt(UseCase.RAG_CHAT, provider, data)


class TestRagChatAnthropic:

    def test_business_context_tag_present(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<business_context>" in user
        assert "</business_context>" in user

    def test_business_data_tag_present(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<business_data>" in user
        assert "</business_data>" in user

    def test_question_tag_present(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<question>" in user
        assert "</question>" in user

    def test_output_format_tag_present(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<output_format>" in user
        assert "</output_format>" in user

    def test_revenue_tag_present_when_data_supplied(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<revenue>" in user

    def test_staff_tag_present_when_data_supplied(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<staff_performance>" in user

    def test_no_data_tags_when_minimal(self, minimal_rag_data):
        _, user = _build(Provider.ANTHROPIC, minimal_rag_data)
        assert "<business_data>" not in user
        assert "<revenue>" not in user

    def test_revenue_values_rendered(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "9,200" in user
        assert "13,100" in user

    def test_decline_arrow_rendered(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "▼" in user

    def test_cancellation_rate_rendered(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "18%" in user

    def test_staff_note_rendered(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "(new)" in user

    def test_campaign_roi_rendered(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "ROI" in user

    def test_system_has_role_block(self, full_rag_data):
        sys, _ = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<role>" in sys

    def test_system_has_rules_block(self, full_rag_data):
        sys, _ = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<rules>" in sys

    def test_system_no_hallucination_guardrail(self, full_rag_data):
        sys, _ = _build(Provider.ANTHROPIC, full_rag_data)
        assert "not invent" in sys.lower() or "do not invent" in sys.lower()

    def test_json_schema_fields_in_output_format(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        for field in ["summary", "root_causes", "supporting_data",
                      "recommendations", "confidence", "data_gaps"]:
            assert field in user

    def test_question_text_in_question_block(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert full_rag_data.question in user

    def test_business_id_in_context(self, full_rag_data):
        _, user = _build(Provider.ANTHROPIC, full_rag_data)
        assert "salon_123" in user


class TestRagChatOpenAI:

    def test_business_context_section_present(self, full_rag_data):
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert "Business Context" in user

    def test_no_xml_tags_in_user_prompt(self, full_rag_data):
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert "<business_data>" not in user
        assert "<question>" not in user

    def test_markdown_headers_used(self, full_rag_data):
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert "##" in user

    def test_revenue_values_rendered(self, full_rag_data):
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert "9,200" in user

    def test_question_in_user_prompt(self, full_rag_data):
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert full_rag_data.question in user

    def test_no_output_format_block_in_user(self, full_rag_data):
        """OpenAI relies on response_format — no schema block needed in prompt."""
        _, user = _build(Provider.OPENAI, full_rag_data)
        assert "<output_format>" not in user

    def test_system_contains_json_schema(self, full_rag_data):
        """Schema is in the system prompt for OpenAI."""
        sys, _ = _build(Provider.OPENAI, full_rag_data)
        assert "summary" in sys
        assert "root_causes" in sys

    def test_no_data_section_when_minimal(self, minimal_rag_data):
        _, user = _build(Provider.OPENAI, minimal_rag_data)
        assert "## Revenue" not in user
        assert "## Staff" not in user


class TestRagChatProviderDifferences:

    def test_anthropic_longer_system_prompt(self, full_rag_data):
        sys_a, _ = _build(Provider.ANTHROPIC, full_rag_data)
        sys_o, _ = _build(Provider.OPENAI,    full_rag_data)
        assert len(sys_a) > len(sys_o)

    def test_anthropic_user_has_xml_structure(self, full_rag_data):
        _, user_a = _build(Provider.ANTHROPIC, full_rag_data)
        assert "<business_context>" in user_a

    def test_openai_user_has_markdown_structure(self, full_rag_data):
        _, user_o = _build(Provider.OPENAI, full_rag_data)
        assert "##" in user_o
        assert "<business_context>" not in user_o
