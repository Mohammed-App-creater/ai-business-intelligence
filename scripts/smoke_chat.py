"""Smoke test for LLM retry behavior. Not part of the test suite — run manually.

Usage:
    PYTHONPATH=. python -m scripts.smoke_chat "your question here"

To exercise retry paths:
    ANTHROPIC_API_KEY=sk-ant-invalid PYTHONPATH=. python -m scripts.smoke_chat "test"
    ANTHROPIC_BASE_URL=https://10.255.255.1 PYTHONPATH=. python -m scripts.smoke_chat "test"
"""
import asyncio
import logging
import sys
import time

from app.services.llm.llm_gateway import LLMGateway
from app.services.llm.types import UseCase


def configure_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


async def main(question: str) -> int:
    configure_logging()
    gateway = LLMGateway.from_env()

    start = time.monotonic()
    try:
        response = await gateway.call(
            use_case=UseCase.RAG_CHAT,
            system="You are a helpful assistant. Reply briefly.",
            user=question,
            business_id="smoke-test",
        )
        elapsed = time.monotonic() - start
        print(f"\n[OK] {elapsed:.2f}s — was_retried={response.was_retried}")
        print(f"     {str(response.content)[:200]}")
        return 0
    except Exception as exc:
        elapsed = time.monotonic() - start
        print(f"\n[FAIL] {elapsed:.2f}s — {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    question = sys.argv[1] if len(sys.argv) > 1 else "hello"
    sys.exit(asyncio.run(main(question)))
