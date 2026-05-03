"""
scripts/runners/run_appointments_docs.py
=======================================
Single-domain pipeline: **appointments** — analytics → warehouse (`wh_appt_*`) →
chunk text → embed → pgvector.

Uses ``DocGenerator.generate_domain(domain="appointments", ...)`` — the same
entry point as ``scripts/embed_documents.py`` uses via ``generate_all``, but
restricted to one domain for UAT pre-flight.

Does **not** replace ``run_appointments_etl.py`` (verification-only, no pool).

Usage:
    python scripts/runners/run_appointments_docs.py --org-id 42 --start 2025-10-01 --months 6

    python scripts/runners/run_appointments_docs.py --org-id 42 --start 2025-10-01 --force
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date
from pathlib import Path

# ── Repo root on sys.path (same pattern as run_appointments_etl.py) ───────────
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)

# ---------------------------------------------------------------------------
# Preflight — required env for warehouse + vector + analytics + embed + LLM
# (LLM gateway is constructed like embed_documents.py even though appointments
#  chunks do not call the LLM today.)
# ---------------------------------------------------------------------------

_KNOWN_PLACEHOLDER_VALUES: frozenset[str] = frozenset(
    {
        "your_anthropic_api_key_here",
        "your_voyage_api_key_here",
        "putstrongpasswordhere",
        "changeme",
    }
)


def _is_placeholder(value: str) -> bool:
    if not value.strip():
        return True
    return value.strip().lower() in _KNOWN_PLACEHOLDER_VALUES


def _require_non_placeholder(name: str, value: str | None) -> None:
    if value is None:
        raise SystemExit(f"Missing required environment variable: {name}")
    v = value.strip()
    if not v:
        raise SystemExit(f"Missing required environment variable: {name}")
    if _is_placeholder(v):
        raise SystemExit(
            f"{name} is empty or looks like a placeholder; set a real value for UAT."
        )


def validate_env() -> None:
    """Exit with a clear message if required configuration is missing."""
    _require_non_placeholder("SAAS_API_BASE", os.environ.get("SAAS_API_BASE"))
    _require_non_placeholder("ANALYTICS_BACKEND_URL", os.environ.get("ANALYTICS_BACKEND_URL"))

    for key in (
        "WH_PG_USER",
        "WH_PG_PASSWORD",
        "WH_PG_NAME",
        "VEC_PG_USER",
        "VEC_PG_PASSWORD",
        "VEC_PG_NAME",
    ):
        _require_non_placeholder(key, os.environ.get(key))

    provider = os.getenv("EMBEDDING_PROVIDER", "voyage").lower().strip()
    if provider == "openai":
        _require_non_placeholder("OPENAI_API_KEY", os.environ.get("OPENAI_API_KEY"))
    else:
        _require_non_placeholder("VOYAGE_API_KEY", os.environ.get("VOYAGE_API_KEY"))

    llm = os.getenv("LLM_PROVIDER", "anthropic").lower().strip()
    if llm == "openai":
        _require_non_placeholder("OPENAI_API_KEY", os.environ.get("OPENAI_API_KEY"))
    else:
        _require_non_placeholder("ANTHROPIC_API_KEY", os.environ.get("ANTHROPIC_API_KEY"))


async def run(org_id: int, period_start: date, months: int, force: bool) -> None:
    validate_env()

    from app.services.db.db_pool import PGPool, PGTarget
    from app.services.db.warehouse_client import WarehouseClient
    from app.services.doc_generators import DocGenerator
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.vector_store import VectorStore

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        gateway = LLMGateway.from_env()
        emb = EmbeddingClient.from_env()
        wh = WarehouseClient(wh_pool)
        vs = VectorStore(vec_pool)
        gen = DocGenerator(wh, gateway, emb, vs)

        created, skipped, failed = await gen.generate_domain(
            org_id=org_id,
            domain="appointments",
            period_start=period_start,
            months=months,
            force=force,
        )

        print("")
        print("=" * 62)
        print("  Appointments domain — generate_domain result")
        print("=" * 62)
        print(f"  org_id        : {org_id}")
        print(f"  period_start  : {period_start.isoformat()}")
        print(f"  months        : {months}")
        print(f"  force         : {force}")
        print("-" * 62)
        print(f"  docs_created  : {created}")
        print(f"  docs_skipped  : {skipped}")
        print(f"  docs_failed   : {failed}")
        print("-" * 62)
        print(
            "  Note: counts are totals for the appointments domain run "
            "(all appt_* doc types combined), not per doc_type."
        )
        print("=" * 62)
    finally:
        await wh_pool.close()
        await vec_pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run appointments warehouse + embed pipeline for one org (single domain)."
    )
    parser.add_argument("--org-id", type=int, required=True, help="Business / org id (integer)")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        required=True,
        help="Period window anchor — first month included (YYYY-MM-DD, normalized to month start internally)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of calendar months to include from period_start (default: 6)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-embed even when doc_id already exists in the vector store",
    )
    args = parser.parse_args()

    asyncio.run(
        run(
            org_id=args.org_id,
            period_start=args.start,
            months=max(1, args.months),
            force=args.force,
        )
    )


if __name__ == "__main__":
    main()
