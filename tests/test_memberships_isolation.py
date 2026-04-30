"""
tests/test_memberships_isolation.py
====================================
Step 5 — Single tenant-isolation check for memberships docs.

Asserts three things in sequence:
  1. ROUTING:   the question matches memberships keywords
                (otherwise the isolation check is meaningless)
  2. SANITY:    tenant 99 (which has 89 memberships docs) DOES
                retrieve memberships docs for the question
  3. ISOLATION: tenant 42 (no memberships docs) retrieves ZERO
                memberships docs — proving no cross-tenant leakage

Run:
    PYTHONPATH=. python tests/test_memberships_isolation.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

# Must load .env BEFORE PGPool.from_env reads VEC_PG_* environment variables.
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=True)

from app.services.db.db_pool import PGPool, PGTarget
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.query_analyzer import QueryAnalyzer
from app.services.retriever import Retriever
from app.services.vector_store import VectorStore


# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────

TEST_TENANT  = "99"   # has 89 memberships docs (32 unit + 57 monthly)
WRONG_TENANT = "42"   # has revenue/etc but NO memberships docs

QUESTION = "How many active memberships do I have right now?"


# ─────────────────────────────────────────────────────────────────────────────
#  Test
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        emb       = EmbeddingClient.from_env()
        vs        = VectorStore(pool=pool)
        analyzer  = QueryAnalyzer()
        retriever = Retriever(embedding_client=emb, vector_store=vs)

        print(f"Question: {QUESTION!r}\n")
        print("=" * 70)

        # ── 1. ROUTING CHECK ────────────────────────────────────────────────
        print("[1/3] Routing check — question must match memberships keywords")
        analysis = await analyzer.analyze(QUESTION, business_id=WRONG_TENANT)
        print(f"      Route:           {analysis.route.value}")
        print(f"      Method:          {analysis.method}")
        print(f"      Confidence:      {analysis.confidence:.2f}")
        print(f"      Matched kws:     {analysis.matched_keywords[:8]}"
              + (" ..." if len(analysis.matched_keywords) > 8 else ""))

        membership_kws = [
            kw for kw in analysis.matched_keywords
            if "member" in kw.lower()
            or "mrr" in kw.lower()
            or "recurring" in kw.lower()
            or "subscription" in kw.lower()
        ]
        if not membership_kws:
            print("\n      ❌ ROUTING FAILED — no memberships-shaped keywords matched.")
            print(f"         Full match list: {analysis.matched_keywords}")
            print(f"         Add the question's vocabulary to the memberships group.")
            return 1
        print(f"      ✓ memberships kws matched: {membership_kws}\n")

        # ── 2. SANITY: tenant 99 SHOULD see memberships docs ───────────────
        print(f"[2/3] Sanity — tenant {TEST_TENANT} (HAS data) should retrieve "
              "memberships docs")
        ctx99 = await retriever.retrieve(
            QUESTION, tenant_id=TEST_TENANT, analysis=analysis,
        )
        mem99 = [d for d in ctx99.doc_ids if "memberships" in d]
        print(f"      Total docs:      {ctx99.total_results}")
        print(f"      Domains:         {ctx99.domains_searched}")
        print(f"      Memberships:     {len(mem99)}")
        print(f"      Sample mem IDs:  {mem99[:3]}")

        if not mem99:
            print(f"\n      ❌ SANITY FAILED — tenant {TEST_TENANT} should see "
                  "memberships docs.")
            print(f"         Either the docs aren't embedded, or the retriever "
                  "isn't searching memberships.")
            return 1
        print(f"      ✓ tenant {TEST_TENANT} retrieves memberships docs as expected\n")

        # ── 3. ISOLATION: tenant 42 must see ZERO memberships docs ─────────
        print(f"[3/3] Isolation — tenant {WRONG_TENANT} (NO data) must retrieve "
              "0 memberships docs")
        ctx42 = await retriever.retrieve(
            QUESTION, tenant_id=WRONG_TENANT, analysis=analysis,
        )
        mem42 = [d for d in ctx42.doc_ids if "memberships" in d]
        print(f"      Total docs:      {ctx42.total_results}")
        print(f"      Domains:         {ctx42.domains_searched}")
        print(f"      Memberships:     {len(mem42)}")

        if mem42:
            print(f"\n      ❌ ISOLATION VIOLATION — tenant {WRONG_TENANT} received "
                  f"{len(mem42)} memberships docs.")
            print(f"         Leaked IDs: {mem42}")
            print(f"         The vector store is NOT enforcing tenant_id filtering.")
            return 1
        print(f"      ✓ tenant {WRONG_TENANT} received 0 memberships docs — "
              "isolation OK\n")

        print("=" * 70)
        print("✅ Step 5 isolation check PASSED")
        return 0

    finally:
        await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))