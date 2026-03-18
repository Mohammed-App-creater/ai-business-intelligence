"""
Tests for classifier prompts — both providers.
"""
import pytest
from app.prompts import build_prompt
from app.prompts.types import ClassifierData
from app.services.llm.types import UseCase, Provider


def _build(provider, question="Why did my revenue drop?"):
    return build_prompt(UseCase.CLASSIFIER, provider, ClassifierData(question=question))


class TestClassifierAnthropic:

    def test_question_in_user_prompt(self):
        _, user = _build(Provider.ANTHROPIC, "Why did my revenue drop?")
        assert "Why did my revenue drop?" in user

    def test_xml_tag_wraps_question(self):
        _, user = _build(Provider.ANTHROPIC, "test question")
        assert "<question>" in user
        assert "</question>" in user

    def test_system_contains_xml_rules_block(self):
        sys, _ = _build(Provider.ANTHROPIC)
        assert "<rules>" in sys
        assert "</rules>" in sys

    def test_system_defines_rag_and_direct_routes(self):
        sys, _ = _build(Provider.ANTHROPIC)
        assert "RAG" in sys
        assert "DIRECT" in sys

    def test_system_instructs_json_only_response(self):
        sys, _ = _build(Provider.ANTHROPIC)
        assert "JSON" in sys

    def test_system_includes_chain_of_thought_nudge(self):
        sys, _ = _build(Provider.ANTHROPIC)
        assert "step" in sys.lower()

    def test_json_schema_in_system(self):
        sys, _ = _build(Provider.ANTHROPIC)
        assert "confidence" in sys
        assert "reasoning" in sys
        assert "route" in sys


class TestClassifierOpenAI:

    def test_question_in_user_prompt(self):
        _, user = _build(Provider.OPENAI, "How can salons reduce no-shows?")
        assert "How can salons reduce no-shows?" in user

    def test_user_prompt_is_plain_question(self):
        """OpenAI prompt: user turn is the raw question — no XML tags."""
        _, user = _build(Provider.OPENAI, "plain question")
        assert "<question>" not in user
        assert user.strip() == "plain question"

    def test_system_defines_rag_and_direct_routes(self):
        sys, _ = _build(Provider.OPENAI)
        assert "RAG" in sys
        assert "DIRECT" in sys

    def test_system_is_concise(self):
        """OpenAI system prompt should be shorter than Anthropic's."""
        sys_a, _ = _build(Provider.ANTHROPIC)
        sys_o, _ = _build(Provider.OPENAI)
        assert len(sys_o) < len(sys_a)

    def test_no_xml_tags_in_openai_system(self):
        sys, _ = _build(Provider.OPENAI)
        assert "<rules>" not in sys
        assert "<role>" not in sys


class TestClassifierProviderDifferences:

    def test_anthropic_uses_xml_user_turn(self):
        _, user_a = _build(Provider.ANTHROPIC, "Q?")
        assert "<question>" in user_a

    def test_openai_uses_plain_user_turn(self):
        _, user_o = _build(Provider.OPENAI, "Q?")
        assert "<question>" not in user_o

    def test_both_include_route_definitions(self):
        for provider in [Provider.ANTHROPIC, Provider.OPENAI]:
            sys, _ = _build(provider)
            assert "RAG" in sys and "DIRECT" in sys
