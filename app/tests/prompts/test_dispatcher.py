"""
test_dispatcher.py
==================
Tests for the build_prompt dispatcher — routing, type safety, error cases.
"""
import pytest
from app.prompts import build_prompt
from app.prompts.types import ClassifierData, RagChatData, DocGenData
from app.services.llm.types import UseCase, Provider


class TestRouting:

    def test_returns_tuple_of_two_strings(self, classifier_data):
        result = build_prompt(UseCase.CLASSIFIER, Provider.ANTHROPIC, classifier_data)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(s, str) for s in result)

    def test_all_six_combinations_registered(self, classifier_data, full_rag_data, full_doc_data):
        combinations = [
            (UseCase.CLASSIFIER,     Provider.ANTHROPIC, classifier_data),
            (UseCase.CLASSIFIER,     Provider.OPENAI,    classifier_data),
            (UseCase.RAG_CHAT,       Provider.ANTHROPIC, full_rag_data),
            (UseCase.RAG_CHAT,       Provider.OPENAI,    full_rag_data),
            (UseCase.DOC_GENERATION, Provider.ANTHROPIC, full_doc_data),
            (UseCase.DOC_GENERATION, Provider.OPENAI,    full_doc_data),
        ]
        for use_case, provider, data in combinations:
            sys, usr = build_prompt(use_case, provider, data)
            assert sys.strip(), f"Empty system prompt for {use_case}/{provider}"
            assert usr.strip(), f"Empty user prompt for {use_case}/{provider}"

    def test_different_providers_produce_different_prompts(self, classifier_data):
        sys_a, usr_a = build_prompt(UseCase.CLASSIFIER, Provider.ANTHROPIC, classifier_data)
        sys_o, usr_o = build_prompt(UseCase.CLASSIFIER, Provider.OPENAI,    classifier_data)
        # Prompts must differ between providers
        assert sys_a != sys_o

    def test_agent_use_case_raises_value_error(self, classifier_data):
        """AGENT use case has no prompt registered yet — must raise clearly."""
        with pytest.raises(ValueError, match="No prompt registered"):
            build_prompt(UseCase.AGENT, Provider.ANTHROPIC, classifier_data)


class TestTypeSafety:

    def test_wrong_data_type_raises_type_error(self, full_rag_data):
        with pytest.raises(TypeError, match="ClassifierData"):
            build_prompt(UseCase.CLASSIFIER, Provider.ANTHROPIC, full_rag_data)

    def test_doc_data_for_rag_use_case_raises_type_error(self, full_doc_data):
        with pytest.raises(TypeError, match="RagChatData"):
            build_prompt(UseCase.RAG_CHAT, Provider.ANTHROPIC, full_doc_data)

    def test_correct_type_does_not_raise(self, classifier_data):
        result = build_prompt(UseCase.CLASSIFIER, Provider.ANTHROPIC, classifier_data)
        assert result is not None
