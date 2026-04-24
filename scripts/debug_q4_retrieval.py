"""
scripts/debug_q4_retrieval.py
==============================
Run Q4 ("How many expense transactions did I record last month?") and
print EXACTLY what context the LLM received.

Why Q4: fixture answer is 18 (March). AI is returning "150" — a number
that appears in ZERO chunks. Top_k bump from 5→15 didn't help.

We need to see:
  - How many sources the chat endpoint returned
  - What doc_type + period each source is
  - What the chunk_text actually contains
  - Whether there's a chunk with "18 transactions" in the LLM's context

Usage:
    PYTHONPATH=. python scripts/debug_q4_retrieval.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx
from app.services.db.db_pool import PGPool, PGTarget


CHAT_ENDPOINT = "http://localhost:8000/api/v1/chat"
BUSINESS_ID   = "42"

QUESTIONS_TO_DEBUG = [
    ("Q4",  "How many expense transactions did I record last month?"),
    ("Q8",  "How does this quarter's total outflow compare to last quarter?"),
    ("Q15", "Which payment method do I use most often for business bills?"),
    ("Q16", "Which branch costs more to run — Main St or Westside?"),
]


async def main():
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    for qid, question in QUESTIONS_TO_DEBUG:
        print("\n" + "=" * 78)
        print(f"  DEBUG {qid}: {question}")
        print("=" * 78)

        # 1. Hit the chat endpoint
        async with httpx.AsyncClient(timeout=45.0) as client:
            resp = await client.post(
                CHAT_ENDPOINT,
                json={
                    "business_id": BUSINESS_ID,
                    "org_id":      BUSINESS_ID,
                    "question":    question,
                },
            )
            body = resp.json()

        answer = body.get("answer", "")
        route = body.get("route", "")
        sources = body.get("sources", [])

        print(f"\n  route      : {route}")
        print(f"  # sources  : {len(sources)}")
        print(f"  answer     : {answer[:300]}")
        print(f"\n  source IDs : {sources}")

        if not sources:
            print("  ⚠️  NO SOURCES — retrieval returned nothing")
            continue

        # 2. Map source IDs back to chunks
        print(f"\n  ── Retrieved chunks ──")
        async with vec_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    doc_id,
                    doc_type,
                    COALESCE(metadata->>'period', metadata->>'period_start', '-') AS period,
                    LEFT(chunk_text, 200) AS preview
                FROM embeddings
                WHERE doc_id = ANY($1::text[])
                  AND tenant_id = $2
                """,
                sources, BUSINESS_ID,
            )

        # Preserve retrieval order (sources list is already ordered by similarity)
        by_id = {r["doc_id"]: r for r in rows}
        for rank, doc_id in enumerate(sources, 1):
            r = by_id.get(doc_id)
            if not r:
                print(f"    {rank:>2}. {doc_id[:16]}...  ⚠️  NOT FOUND in pgvector for tenant {BUSINESS_ID}")
                continue
            print(f"    {rank:>2}. {r['doc_type']:<28s} period={r['period']:<12s}")
            print(f"        preview: {r['preview'][:150]}")

        # 3. Check specific target content per question
        print(f"\n  ── Target-content check ──")
        if qid == "Q4":
            # Is the March monthly summary chunk in the sources?
            march_chunk = next(
                (by_id.get(sid) for sid in sources
                 if by_id.get(sid) and by_id[sid]["doc_type"] == "exp_monthly_summary"
                 and "2026-03" in str(by_id[sid]["period"])),
                None,
            )
            if march_chunk:
                has_18 = "18 transactions" in march_chunk["preview"] or "across 18" in march_chunk["preview"]
                print(f"    March 2026 monthly_summary in sources: ✅  YES")
                print(f"    Chunk contains '18 transactions'    : {'✅' if has_18 else '❌'}  "
                      f"{'(visible to LLM)' if has_18 else '(NOT in preview — may be beyond 200 chars)'}")
            else:
                print(f"    March 2026 monthly_summary in sources: ❌  NO")
                print(f"    This explains why AI hallucinated '150' — it had no monthly chunk")

        elif qid == "Q8":
            # Does any chunk contain the QoQ figures ($12,810, $13,800, -7.17%)?
            qoq_signals = ["12,810", "13,800", "7.17", "qoq", "q1 2026", "q4 2025"]
            hits = []
            for doc_id in sources:
                r = by_id.get(doc_id)
                if r and any(s.lower() in r["preview"].lower() for s in qoq_signals):
                    hits.append((r["doc_type"], r["period"]))
            if hits:
                print(f"    Chunks with QoQ data in sources: ✅  {len(hits)}")
                for t, p in hits[:3]:
                    print(f"        {t} period={p}")
            else:
                print(f"    Chunks with QoQ data in sources: ❌  NONE")
                print(f"    AI had to hallucinate — QoQ chunk wasn't in top-{len(sources)}")

        elif qid == "Q15":
            # Count payment type chunks in sources
            pay_chunks = [doc_id for doc_id in sources
                          if by_id.get(doc_id)
                          and by_id[doc_id]["doc_type"] == "exp_payment_type_monthly"]
            print(f"    Payment-type chunks in sources: {len(pay_chunks)} (of 18 total in corpus)")
            if pay_chunks:
                methods = set()
                for doc_id in pay_chunks:
                    preview = by_id[doc_id]["preview"].lower()
                    if "cash" in preview: methods.add("cash")
                    if "card" in preview: methods.add("card")
                    if "check" in preview: methods.add("check")
                print(f"    Methods represented: {sorted(methods)}")

        elif qid == "Q16":
            # Count location chunks per location
            loc_chunks = {}
            for doc_id in sources:
                r = by_id.get(doc_id)
                if r and r["doc_type"] in ("exp_location_monthly", "exp_cat_location_cross"):
                    preview = r["preview"].lower()
                    if "main st" in preview or "main-st" in preview:
                        loc_chunks.setdefault("main_st", []).append(doc_id)
                    if "westside" in preview:
                        loc_chunks.setdefault("westside", []).append(doc_id)
            print(f"    Main St chunks : {len(loc_chunks.get('main_st', []))}")
            print(f"    Westside chunks: {len(loc_chunks.get('westside', []))}")
            if not loc_chunks.get("main_st"):
                print(f"    ❌ No Main St chunk in sources — explains 'no data for Main St'")

    await vec_pool.close()

    print("\n" + "=" * 78)
    print("  Next step interpretation")
    print("=" * 78)
    print("""
  - If the correct chunks ARE in sources but LLM still refuses:
      → LLM-quality / prompt-construction issue (chunks are being ignored)
      → Fix: reranker, chunk-order bias, or explicit prompt directive

  - If the correct chunks are NOT in sources (retrieval missed them):
      → Retrieval ranking issue — top-15 still not enough, or cosine
        similarity isn't matching well on these question types
      → Fix: try top_k=25, or add query rewriting for specific patterns

  - If chunks are in sources but truncated at 200 chars here:
      → Just our preview display; LLM sees full text. Not a real issue.

  - If chunks are listed but with wrong tenant_id filter behavior:
      → Tenant leak (shouldn't happen given isolation tests pass)
""")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)
    