"""
test_doc_generation_prompts.py
==============================
Tests for doc_generation prompts — both providers.
"""
import pytest
from app.prompts import build_prompt
from app.prompts.types import DocGenData
from app.services.llm.types import UseCase, Provider


def _build(provider, data):
    return build_prompt(UseCase.DOC_GENERATION, provider, data)


class TestDocGenAnthropic:

    def test_kpi_data_xml_tag_present(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "<kpi_data>" in user
        assert "</kpi_data>" in user

    def test_revenue_in_user_prompt(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "9,200" in user

    def test_prev_revenue_comparison_rendered(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "13,100" in user
        assert "▼" in user

    def test_cancellation_rate_rendered(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "18%" in user

    def test_top_staff_rendered(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "Sarah" in user

    def test_top_service_rendered(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "Facial Treatment" in user

    def test_extra_notes_rendered(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "Mia joined" in user

    def test_minimal_data_no_crash(self, minimal_doc_data):
        _, user = _build(Provider.ANTHROPIC, minimal_doc_data)
        assert user.strip()

    def test_context_block_present(self, minimal_doc_data):
        _, user = _build(Provider.ANTHROPIC, minimal_doc_data)
        assert "<context>" in user

    def test_system_instructs_prose_not_bullets(self, full_doc_data):
        sys, _ = _build(Provider.ANTHROPIC, full_doc_data)
        assert "bullet" in sys.lower() or "prose" in sys.lower()

    def test_system_has_role_block(self, full_doc_data):
        sys, _ = _build(Provider.ANTHROPIC, full_doc_data)
        assert "<role>" in sys

    def test_system_instructs_two_to_three_sentences(self, full_doc_data):
        sys, _ = _build(Provider.ANTHROPIC, full_doc_data)
        assert "2-3" in sys or "2–3" in sys

    def test_user_ends_with_instruction(self, full_doc_data):
        _, user = _build(Provider.ANTHROPIC, full_doc_data)
        assert "summary" in user.lower()
        assert "notable insight" in user.lower()


class TestDocGenOpenAI:

    def test_no_kpi_xml_wrapper(self, full_doc_data):
        _, user = _build(Provider.OPENAI, full_doc_data)
        assert "<kpi_data>" not in user

    def test_revenue_in_user_prompt(self, full_doc_data):
        _, user = _build(Provider.OPENAI, full_doc_data)
        assert "9,200" in user

    def test_decline_arrow_rendered(self, full_doc_data):
        _, user = _build(Provider.OPENAI, full_doc_data)
        assert "▼" in user

    def test_top_staff_rendered(self, full_doc_data):
        _, user = _build(Provider.OPENAI, full_doc_data)
        assert "Sarah" in user

    def test_system_instructs_prose(self, full_doc_data):
        sys, _ = _build(Provider.OPENAI, full_doc_data)
        assert "prose" in sys.lower() or "bullet" in sys.lower()

    def test_minimal_data_no_crash(self, minimal_doc_data):
        _, user = _build(Provider.OPENAI, minimal_doc_data)
        assert user.strip()

    def test_business_id_in_user(self, minimal_doc_data):
        _, user = _build(Provider.OPENAI, minimal_doc_data)
        assert "salon_123" in user


class TestDocGenProviderDifferences:

    def test_anthropic_uses_kpi_data_block(self, full_doc_data):
        _, user_a = _build(Provider.ANTHROPIC, full_doc_data)
        assert "<kpi_data>" in user_a

    def test_openai_uses_plain_kpi_block(self, full_doc_data):
        _, user_o = _build(Provider.OPENAI, full_doc_data)
        assert "<kpi_data>" not in user_o

    def test_both_render_same_key_values(self, full_doc_data):
        _, user_a = _build(Provider.ANTHROPIC, full_doc_data)
        _, user_o = _build(Provider.OPENAI,    full_doc_data)
        for value in ["9,200", "Sarah", "18%"]:
            assert value in user_a, f"Missing {value} in Anthropic"
            assert value in user_o, f"Missing {value} in OpenAI"
