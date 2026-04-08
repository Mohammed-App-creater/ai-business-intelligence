"""
embed_documents.py
==================
CLI entry point for the document generation + embedding pipeline.

Reads from the analytics warehouse, generates human-readable summaries,
embeds them, and stores them in the vector store.

Usage
-----
    python scripts/embed_documents.py                    # all orgs, 3 months
    python scripts/embed_documents.py --org-id 42
    python scripts/embed_documents.py --months 6
    python scripts/embed_documents.py --domain revenue
    python scripts/embed_documents.py --force
    python scripts/embed_documents.py --dry-run
"""
from __future__ import annotations
from app.services.doc_generator.domains.revenue import generate_revenue_docs

import argparse
import asyncio
import logging

from app.services.db.db_pool import PGPool, PGTarget, DBPool, DBTarget
from app.services.db.warehouse_client import WarehouseClient
from app.services.doc_generator import DocGenerator
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.llm.llm_gateway import LLMGateway
from app.services.vector_store import VectorStore


async def run(args) -> None:
    prod_pool = await DBPool.from_env(DBTarget.PRODUCTION)
    wh_pool   = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool  = await PGPool.from_env(PGTarget.VECTOR)

    gateway   = LLMGateway.from_env()
    emb       = EmbeddingClient.from_env()
    wh        = WarehouseClient(wh_pool)
    vs        = VectorStore(vec_pool)

    if args.org_id:
        org_ids = [args.org_id]
    else:
        async with prod_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT Id FROM tbl_organization "
                    "WHERE ClientStatus=1 AND IsDeleted=b'1' ORDER BY Id"
                )
                rows = await cur.fetchall()
        org_ids = [r["Id"] for r in rows]

    logging.info("embed_documents | orgs=%d | months=%d", len(org_ids), args.months)

    if args.dry_run:
        logging.info("embed_documents dry-run — skipping generation")
        await prod_pool.close()
        await wh_pool.close()
        await vec_pool.close()
        return

    total_created = total_skipped = total_failed = 0

    for org_id in org_ids:
        gen = DocGenerator(wh, gateway, emb, vs)
        result = await gen.generate_all(
            org_id=org_id,
            period_start=None,
            months=args.months,
            domain=args.domain,
            force=args.force,
        )
        total_created += result["docs_created"]
        total_skipped += result["docs_skipped"]
        total_failed  += result["docs_failed"]
        logging.info("org=%d | created=%d skipped=%d failed=%d",
                     org_id, result["docs_created"],
                     result["docs_skipped"], result["docs_failed"])

    logging.info(
        "embed_documents DONE | created=%d skipped=%d failed=%d",
        total_created, total_skipped, total_failed,
    )
    await prod_pool.close()
    await wh_pool.close()
    await vec_pool.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Generate + embed warehouse documents")
    parser.add_argument("--org-id",  type=int,  default=None)
    parser.add_argument("--months",  type=int,  default=3)
    parser.add_argument("--domain",  type=str,  default=None)
    parser.add_argument("--force",   action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
