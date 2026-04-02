"""
test_query_analyzer_live.py
===========================
Exercise QueryAnalyzer against your real .env (LLM_PROVIDER, OPENAI_API_KEY, etc.).

Modes
-----
  rules       Rule-based routing only (no LLM). Uses QueryAnalyzer(gateway=None).
  classifier  Single LLM call: classifier only (QueryAnalyzer._classifier_check).
  full        Production path: rules first, then classifier if rules are inconclusive.
  trace       Debug: print raw rule step (preview_rule_routing), threshold decision,
              optional classifier + full analyze (use --skip-llm to avoid API calls).

Usage
-----
    # From repo root, with venv active:
    python scripts/test_query_analyzer_live.py --mode rules --question "Hi how are you"
    python scripts/test_query_analyzer_live.py --mode classifier --question "Hi how are you"
    python scripts/test_query_analyzer_live.py --mode full --question "Why did my revenue drop?"
    python scripts/test_query_analyzer_live.py --mode trace --question "Hi how are you"
    python scripts/test_query_analyzer_live.py --mode trace --skip-llm --question "Hi how are you"
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Repo root on sys.path (same pattern as running other scripts from project root)
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")


def _print_env_hint() -> None:
    provider = os.getenv("LLM_PROVIDER", "(unset)")
    print(f"LLM_PROVIDER={provider}")
    if provider.lower() == "openai":
        from app.services.llm.llm_gateway import MODEL_MAP
        from app.services.llm.types import Provider, UseCase

        m = MODEL_MAP[Provider.OPENAI][UseCase.CLASSIFIER]
        print(f"Classifier model (OpenAI): {m}")


async def _run_rules(question: str, business_id: str) -> None:
    from app.services.query_analyzer import QueryAnalyzer

    analyzer = QueryAnalyzer(gateway=None)
    result = await analyzer.analyze(question, business_id)
    print("--- rules only (no LLM) ---")
    print(f"  route={result.route.value} confidence={result.confidence:.2f}")
    print(f"  method={result.method}")
    print(f"  reasoning={result.reasoning!r}")


async def _run_classifier_only(question: str, business_id: str) -> None:
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.query_analyzer import QueryAnalyzer

    gateway = LLMGateway.from_env()
    analyzer = QueryAnalyzer(gateway=gateway)
    print("--- classifier LLM only ---")
    result = await analyzer._classifier_check(question, business_id)
    print(f"  route={result.route.value} confidence={result.confidence:.2f}")
    print(f"  method={result.method}")
    print(f"  reasoning={result.reasoning!r}")


async def _run_full(question: str, business_id: str) -> None:
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.query_analyzer import QueryAnalyzer

    gateway = LLMGateway.from_env()
    analyzer = QueryAnalyzer(gateway=gateway)
    print("--- full analyze (rules + classifier if needed) ---")
    result = await analyzer.analyze(question, business_id)
    print(f"  route={result.route.value} confidence={result.confidence:.2f}")
    print(f"  method={result.method}")
    print(f"  reasoning={result.reasoning!r}")
    print(f"  latency_ms={result.latency_ms:.1f}")


def _print_rule_snapshot(label: str, r) -> None:
    print(label)
    print(f"  route={r.route.value} confidence={r.confidence:.2f}")
    print(f"  method={r.method}")
    print(f"  matched_keywords={r.matched_keywords!r}")
    print(f"  reasoning={r.reasoning!r}")


async def _run_trace(question: str, business_id: str, skip_llm: bool) -> None:
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.query_analyzer import QueryAnalyzer

    gateway = None if skip_llm else LLMGateway.from_env()
    analyzer = QueryAnalyzer(gateway=gateway)

    rules = analyzer.preview_rule_routing(question)
    _print_rule_snapshot("--- Step 1: rules only (preview_rule_routing) ---", rules)

    thr = analyzer.confidence_threshold
    print()
    print(f"confidence_threshold={thr:.2f}  (rule confidence >= threshold skips classifier)")
    if rules.confidence >= thr:
        print("Classifier: SKIPPED (rule confidence is high enough).")
        print("Final route would match Step 1 unless you change thresholds.")
        return

    print("Classifier: WOULD RUN (rule confidence below threshold).")
    if gateway is None:
        print(
            "With gateway=None, analyze() maps this to RAG (rules_fallback) "
            "without calling the LLM."
        )
    print()

    if skip_llm:
        print("--- Step 2: skipped (--skip-llm) ---")
        print("Re-run without --skip-llm to run analyze() (one LLM call if classifier runs).")
        return

    print("--- Step 2: analyze() production path (at most one classifier LLM call) ---")
    final = await analyzer.analyze(question, business_id)
    print(f"  route={final.route.value} confidence={final.confidence:.2f}")
    print(f"  method={final.method}")
    print(f"  reasoning={final.reasoning!r}")
    print(f"  latency_ms={final.latency_ms:.1f}")
    if rules.route != final.route or abs(rules.confidence - final.confidence) > 0.01:
        print(
            "(Step 1 vs Step 2 differ: classifier ran or gateway fallback applied.)"
        )


async def main() -> None:
    p = argparse.ArgumentParser(description="Live QueryAnalyzer / classifier test")
    p.add_argument(
        "--mode",
        choices=("rules", "classifier", "full", "trace"),
        required=True,
        help="rules=no LLM; classifier=one LLM call; full=production; trace=debug chain",
    )
    p.add_argument("--question", default="Hi how are you", help="User question")
    p.add_argument("--business-id", default="1", help="Tenant id for logging/quota")
    p.add_argument(
        "--skip-llm",
        action="store_true",
        help="trace mode: print rules + decisions only, no OpenAI calls",
    )
    args = p.parse_args()

    _print_env_hint()
    print(f"question={args.question!r} business_id={args.business_id!r}")
    if args.mode == "trace":
        print(f"skip_llm={args.skip_llm}")
    print()

    try:
        if args.mode == "rules":
            await _run_rules(args.question, args.business_id)
        elif args.mode == "classifier":
            await _run_classifier_only(args.question, args.business_id)
        elif args.mode == "trace":
            await _run_trace(args.question, args.business_id, args.skip_llm)
        else:
            await _run_full(args.question, args.business_id)
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {type(exc).__name__}: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    asyncio.run(main())
