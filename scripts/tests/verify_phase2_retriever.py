#!/usr/bin/env python3
"""
Phase 2.2 Task C — Retriever smoke test (real embedding + vector store).

Wiring: ``QueryAnalyzer(gateway=None)`` + ``Retriever(embedding, vector_store)``.

``gateway=None`` matches tests that use rule-based routing only; avoids importing
``openai``/``anthropic`` when the system Python has no venv. For full ChatService
parity, set ``gateway=LLMGateway.from_env()`` in an environment with deps.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)

TENANT_ID = "40"
QUESTIONS = [
    "How many appointments did we have last month?",
    "What was our cancellation rate at DemoLocation?",
    "Which staff member completed the most appointments?",
]


async def main() -> None:
    from app.services.db.db_pool import PGPool, PGTarget
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.query_analyzer import QueryAnalyzer
    from app.services.retriever import Retriever
    from app.services.time_parser import parse_since_date
    from app.services.vector_store import VectorStore

    # Warehouse pool optional for this script; open for parity with embed_documents.
    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        analyzer = QueryAnalyzer(gateway=None)
        emb = EmbeddingClient.from_env()
        vs = VectorStore.from_pool(vec_pool)
        retriever = Retriever(emb, vs)

        print("")
        print("=" * 72)
        print("  Phase 2.2 Task C — Retriever smoke (tenant_id=%r)" % TENANT_ID)
        print("=" * 72)

        for q in QUESTIONS:
            analysis = await analyzer.analyze(q, business_id=TENANT_ID)
            since_date = parse_since_date(q)
            ctx = await retriever.retrieve(
                question=q,
                tenant_id=TENANT_ID,
                analysis=analysis,
                since_date=since_date,
            )

            print(f"\n  Question: {q}")
            print(f"  Analyzer route: {analysis.route.value}  confidence={analysis.confidence:.2f}")
            print(f"  matched_keywords (sample): {analysis.matched_keywords[:15]}{'...' if len(analysis.matched_keywords) > 15 else ''}")
            print(f"  since_date: {since_date}")
            print(f"  domains_searched: {ctx.domains_searched}")
            print(f"  total_results (chunks): {ctx.total_results}")
            print(f"  doc_ids ({len(ctx.doc_ids)}):")
            for did in ctx.doc_ids:
                print(f"    - {did}")
            print(f"  chunk previews (first 200 chars each, {len(ctx.documents)} chunks):")
            for i, doc in enumerate(ctx.documents, 1):
                prev = (doc or "").replace("\n", " ")[:200]
                print(f"    [{i}] {prev!r}{'...' if len(doc or '') > 200 else ''}")

            if ctx.total_results == 0 and "appointment" in q.lower():
                print("\n  *** NOTE: zero chunks for an appointments-flavored question — investigate. ***")

        print("\n" + "=" * 72)

    finally:
        await wh_pool.close()
        await vec_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
