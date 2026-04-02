"""
scripts/test_openai_account.py
===============================
Minimal test to verify your OpenAI API key and quota are working.
Sends one real request to gpt-4o-mini using the classifier prompt.

Usage:
    python scripts/test_openai_account.py
    python scripts/test_openai_account.py --question "What was my revenue last month?"
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import openai
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(_REPO_ROOT / ".env", override=True)

# ---------------------------------------------------------------------------
# Classifier prompt (matches prompts/classifier/openai.py exactly)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a routing classifier for a Business Intelligence assistant \
serving beauty and wellness businesses.

Classify the user question into exactly one of:
  RAG    — requires the business's own data (revenue, bookings, staff, clients, trends)
  DIRECT — general knowledge or advice, no business data needed

Respond with JSON: {"route": "RAG" | "DIRECT", "confidence": 0.0-1.0, "reasoning": "one sentence"}"""


async def call_classifier(question: str) -> None:
    client = openai.AsyncOpenAI()  # reads OPENAI_API_KEY from env automatically

    print(f"\nQuestion : {question!r}")
    print(f"Model    : gpt-4o-mini")
    print(f"API key  : {os.getenv('OPENAI_API_KEY', 'NOT SET')[:8]}...")
    print("-" * 50)

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=256,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": question},
            ],
        )

        raw = response.choices[0].message.content
        result = json.loads(raw)

        print("✅  SUCCESS — OpenAI account is working\n")
        print(f"  route      : {result.get('route')}")
        print(f"  confidence : {result.get('confidence')}")
        print(f"  reasoning  : {result.get('reasoning')}")
        print()
        print(f"  input_tokens  : {response.usage.prompt_tokens}")
        print(f"  output_tokens : {response.usage.completion_tokens}")
        print(f"  model used    : {response.model}")

    except openai.AuthenticationError:
        print("❌  AUTHENTICATION ERROR")
        print("    Your OPENAI_API_KEY is invalid or missing.")
        print("    Check: https://platform.openai.com/api-keys")
        sys.exit(1)

    except openai.RateLimitError as exc:
        print("❌  RATE LIMIT / QUOTA ERROR")
        print(f"    {exc}")
        print()
        print("    This is a billing/quota issue, NOT a code bug.")
        print("    Fix: https://platform.openai.com/settings/organization/billing")
        sys.exit(1)

    except openai.APIConnectionError as exc:
        print("❌  CONNECTION ERROR")
        print(f"    {exc}")
        print("    Check your internet / firewall / proxy settings.")
        sys.exit(1)

    except openai.APIStatusError as exc:
        print(f"❌  API ERROR {exc.status_code}")
        print(f"    {exc.message}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test OpenAI account with classifier prompt")
    parser.add_argument(
        "--question", "-q",
        default="Hi how are you",
        help="Question to classify (default: 'Hi how are you')",
    )
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("❌  OPENAI_API_KEY environment variable is not set.")
        sys.exit(1)

    asyncio.run(call_classifier(args.question))


if __name__ == "__main__":
    main()