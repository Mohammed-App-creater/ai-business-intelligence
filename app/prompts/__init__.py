"""
app/prompts/__init__.py
=======================
Single entry point for all prompt construction.

Call sites use only this — never import individual prompt modules directly.

Usage
-----
    from app.prompts import build_prompt
    from app.prompts.types import ClassifierData, RagChatData, DocGenData

    system, user = build_prompt(UseCase.CLASSIFIER, Provider.ANTHROPIC, ClassifierData(question="..."))
    system, user = build_prompt(UseCase.RAG_CHAT,   Provider.OPENAI,    rag_data)
    system, user = build_prompt(UseCase.DOC_GENERATION, Provider.ANTHROPIC, doc_data)
"""
from __future__ import annotations

from app.services.llm.types import Provider, UseCase

from .types import ClassifierData, DocGenData, RagChatData

# ---------------------------------------------------------------------------
# Prompt module registry
# ---------------------------------------------------------------------------

# Each entry: (UseCase, Provider) → module with a build(data) function
_REGISTRY: dict[tuple[UseCase, Provider], object] = {}


def _register() -> None:
    from .classifier    import anthropic as classifier_anthropic
    from .classifier    import openai    as classifier_openai
    from .rag_chat      import anthropic as rag_chat_anthropic
    from .rag_chat      import openai    as rag_chat_openai
    from .doc_generation import anthropic as doc_gen_anthropic
    from .doc_generation import openai    as doc_gen_openai

    _REGISTRY.update({
        (UseCase.CLASSIFIER,     Provider.ANTHROPIC): classifier_anthropic,
        (UseCase.CLASSIFIER,     Provider.OPENAI):    classifier_openai,
        (UseCase.RAG_CHAT,       Provider.ANTHROPIC): rag_chat_anthropic,
        (UseCase.RAG_CHAT,       Provider.OPENAI):    rag_chat_openai,
        (UseCase.DOC_GENERATION, Provider.ANTHROPIC): doc_gen_anthropic,
        (UseCase.DOC_GENERATION, Provider.OPENAI):    doc_gen_openai,
    })


_register()

# Accepted data types per use case
_DATA_TYPES: dict[UseCase, type] = {
    UseCase.CLASSIFIER:     ClassifierData,
    UseCase.RAG_CHAT:       RagChatData,
    UseCase.DOC_GENERATION: DocGenData,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_prompt(
    use_case: UseCase,
    provider: Provider,
    data: ClassifierData | RagChatData | DocGenData,
) -> tuple[str, str]:
    """
    Build and return (system, user) for the given use case and provider.

    Parameters
    ----------
    use_case : Which LLM task this prompt is for.
    provider : Which provider will receive the prompt.
    data     : Typed input data for the prompt — must match the use case.

    Returns
    -------
    (system_prompt, user_prompt) — ready to pass to gateway.call()

    Raises
    ------
    ValueError  if use_case/provider combination has no registered prompt.
    TypeError   if data type does not match the expected type for the use case.
    """
    # Type check
    expected_type = _DATA_TYPES.get(use_case)
    if expected_type and not isinstance(data, expected_type):
        raise TypeError(
            f"UseCase.{use_case.value} expects {expected_type.__name__}, "
            f"got {type(data).__name__}"
        )

    key = (use_case, provider)
    module = _REGISTRY.get(key)

    if module is None:
        raise ValueError(
            f"No prompt registered for use_case={use_case.value!r} "
            f"provider={provider.value!r}. "
            f"Available: {[f'{u.value}/{p.value}' for u, p in _REGISTRY]}"
        )

    return module.build(data)  # type: ignore[attr-defined]


__all__ = ["build_prompt", "ClassifierData", "RagChatData", "DocGenData"]