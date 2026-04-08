"""
app/services/doc_generator/domains/revenue.py
=============================================
Revenue domain document handler for DocGenerator.

Reads the 6 revenue document types produced by the ETL extractor
from the analytics warehouse, generates human-readable chunk_text
for each one, and stores embeddings in pgvector.

Called by DocGenerator.generate_all() when domain="revenue" or domain=None.

Document types handled:
    monthly_summary         → one doc per period
    payment_type_breakdown  → one aggregate doc
    staff_revenue           → one doc per staff member
    location_revenue        → one doc per location per period
    promo_impact            → one aggregate doc
    failed_refunds          → one aggregate doc
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "revenue"

# Doc types this handler owns
REVENUE_DOC_TYPES = {
    "monthly_summary",
    "payment_type_breakdown",
    "staff_revenue",
    "location_revenue",
    "promo_impact",
    "failed_refunds",
}


# ---------------------------------------------------------------------------
# chunk_text generators  (one per doc_type)
# These produce the human-readable text that gets embedded.
# The ETL already writes a `text` field — these enrich it into a
# richer narrative chunk suitable for RAG injection.
# ---------------------------------------------------------------------------

def _chunk_monthly_summary(row: dict) -> str:
    period      = row.get("period", "unknown")
    revenue     = row.get("service_revenue", 0)
    tips        = row.get("total_tips", 0)
    tax         = row.get("total_tax", 0)
    collected   = row.get("total_collected", 0)
    discounts   = row.get("total_discounts", 0)
    gc          = row.get("gc_redemptions", 0)
    visits      = row.get("visit_count", 0)
    avg_ticket  = row.get("avg_ticket", 0)
    mom         = row.get("mom_growth_pct")
    slope       = row.get("trend_slope", 0)
    direction   = row.get("trend_direction", "flat")
    refunds     = row.get("refund_count", 0)
    cancels     = row.get("cancel_count", 0)

    mom_str = (
        f"Revenue changed {mom:+.1f}% compared to the previous period."
        if mom is not None
        else "This is the first recorded period — no prior comparison available."
    )

    trend_str = {
        "up":   "The overall revenue trend is growing.",
        "down": "The overall revenue trend is declining.",
        "flat": "Revenue is holding steady with no significant trend.",
    }.get(direction, "")

    return (
        f"Revenue Summary — {period}\n"
        f"Service revenue: ${revenue:,.2f} across {visits} successful visits.\n"
        f"Average ticket value: ${avg_ticket:,.2f}.\n"
        f"Tips collected: ${tips:,.2f}. Tax collected: ${tax:,.2f}.\n"
        f"Total collected (including tips): ${collected:,.2f}.\n"
        f"Discounts given: ${discounts:,.2f}. Gift card redemptions: ${gc:,.2f}.\n"
        f"Refunds: {refunds}. Cancellations: {cancels}.\n"
        f"{mom_str} {trend_str}"
    ).strip()


def _chunk_payment_type_breakdown(row: dict) -> str:
    period    = row.get("period", "this period")
    breakdown = row.get("breakdown", [])
    lines = [
        f"  {r['payment_type']}: ${r['revenue']:,.2f} ({r['pct_of_total']:.1f}% of total, {r['visit_count']} visits)"
        for r in sorted(breakdown, key=lambda r: r.get("revenue", 0), reverse=True)
    ]
    top = breakdown[0]["payment_type"] if breakdown else "unknown"
    return (
        f"Payment Type Breakdown — {period}\n"
        + "\n".join(lines)
        + f"\nDominant payment method: {top}."
    )


def _chunk_staff_revenue(row: dict) -> str:
    name    = row.get("staff_name", "Unknown")
    period  = row.get("period", "this period")
    rev     = row.get("service_revenue", 0)
    tips    = row.get("tips_collected", 0)
    visits  = row.get("visit_count", 0)
    ticket  = row.get("avg_ticket", 0)
    rank    = row.get("revenue_rank", "?")
    return (
        f"Staff Revenue — {name} — {period}\n"
        f"{name} generated ${rev:,.2f} in service revenue across {visits} visits.\n"
        f"Average ticket: ${ticket:,.2f}. Tips collected: ${tips:,.2f}.\n"
        f"Revenue rank: #{rank} among all staff this period."
    )


def _chunk_location_revenue(row: dict) -> str:
    name    = row.get("location_name", "Unknown Location")
    period  = row.get("period", "unknown")
    rev     = row.get("service_revenue", 0)
    pct     = row.get("pct_of_total_revenue", 0)
    visits  = row.get("visit_count", 0)
    ticket  = row.get("avg_ticket", 0)
    tips    = row.get("total_tips", 0)
    disc    = row.get("total_discounts", 0)
    gc      = row.get("gc_redemptions", 0)
    mom     = row.get("mom_growth_pct")
    mom_str = f"Month-over-month change: {mom:+.1f}%." if mom is not None else ""
    return (
        f"Location Revenue — {name} — {period}\n"
        f"Service revenue: ${rev:,.2f} ({pct:.1f}% of total business revenue).\n"
        f"{visits} visits. Average ticket: ${ticket:,.2f}.\n"
        f"Tips: ${tips:,.2f}. Discounts: ${disc:,.2f}. Gift card redemptions: ${gc:,.2f}.\n"
        f"{mom_str}"
    ).strip()


def _chunk_promo_impact(row: dict) -> str:
    period      = row.get("period", "this period")
    total_disc  = row.get("total_discount_given", 0)
    total_uses  = row.get("total_promo_uses", 0)
    breakdown   = row.get("breakdown", [])
    lines = [
        f"  '{r['promo_code']}' ({r['promo_description']}): "
        f"used {r['times_used']}x at {r.get('location_name','all locations')} "
        f"= ${r['total_discount_given']:,.2f} in discounts"
        for r in breakdown
    ]
    return (
        f"Promo Code Impact — {period}\n"
        f"Total discount given across all promo codes: ${total_disc:,.2f} over {total_uses} uses.\n"
        + "\n".join(lines)
    )


def _chunk_failed_refunds(row: dict) -> str:
    period      = row.get("period", "this period")
    total_lost  = row.get("total_lost_revenue", 0)
    total_vis   = row.get("total_affected_visits", 0)
    breakdown   = row.get("breakdown", [])
    lines = [
        f"  {r['status_label']}: {r['visit_count']} visits = ${r['lost_revenue']:,.2f} lost "
        f"(avg ${r['avg_lost_per_visit']:,.2f}/visit)"
        for r in breakdown
    ]
    return (
        f"Failed and Refunded Visits — {period}\n"
        f"Total lost or reversed revenue: ${total_lost:,.2f} across {total_vis} affected visits.\n"
        + "\n".join(lines)
        + "\nNote: no-show cost requires cross-referencing appointment data (not included here)."
    )


CHUNK_GENERATORS = {
    "monthly_summary":        _chunk_monthly_summary,
    "payment_type_breakdown": _chunk_payment_type_breakdown,
    "staff_revenue":          _chunk_staff_revenue,
    "location_revenue":       _chunk_location_revenue,
    "promo_impact":           _chunk_promo_impact,
    "failed_refunds":         _chunk_failed_refunds,
}


# ---------------------------------------------------------------------------
# doc_id — stable, content-addressable
# ---------------------------------------------------------------------------

def _make_doc_id(org_id: int, doc_type: str, row: dict) -> str:
    """
    Deterministic doc_id so we can skip re-embedding unchanged documents
    and upsert correctly on re-runs.

    Format: revenue:{org_id}:{doc_type}:{discriminator}
    """
    discriminator_parts = [str(org_id), doc_type]

    if "period" in row:
        discriminator_parts.append(row["period"])
    if "emp_id" in row:
        discriminator_parts.append(str(row["emp_id"]))
    if "location_id" in row:
        discriminator_parts.append(str(row["location_id"]))

    base = ":".join(discriminator_parts)
    # Hash for uniqueness without exposing internal keys
    h = hashlib.sha256(base.encode()).hexdigest()[:12]
    return f"revenue:{org_id}:{doc_type}:{h}"


# ---------------------------------------------------------------------------
# Main handler — called by DocGenerator
# ---------------------------------------------------------------------------

async def generate_revenue_docs(
    org_id: int,
    warehouse_rows: list[dict],
    embedding_client: Any,
    vector_store: Any,
    force: bool = False,
) -> dict[str, int]:
    """
    Generate and embed all revenue documents for one org.

    Parameters
    ----------
    org_id:
        The tenant / business ID.
    warehouse_rows:
        Documents produced by RevenueExtractor.run() and stored in
        the analytics warehouse. Each row must have a ``doc_type`` field.
    embedding_client:
        EmbeddingClient instance — used to embed chunk_text.
    vector_store:
        VectorStore instance — used to upsert embeddings.
    force:
        If True, re-embed even if the doc_id already exists in the vector store.

    Returns
    -------
    dict with keys: docs_created, docs_skipped, docs_failed
    """
    created = skipped = failed = 0

    for row in warehouse_rows:
        doc_type = row.get("doc_type")

        if doc_type not in REVENUE_DOC_TYPES:
            logger.debug("revenue handler: skipping unknown doc_type=%s", doc_type)
            continue

        doc_id = _make_doc_id(org_id, doc_type, row)

        # Skip if already embedded (unless --force)
        if not force and await vector_store.exists(doc_id):
            skipped += 1
            continue

        # Generate rich chunk text
        chunk_fn = CHUNK_GENERATORS.get(doc_type)
        if chunk_fn is None:
            logger.warning("revenue handler: no chunk generator for doc_type=%s", doc_type)
            failed += 1
            continue

        try:
            chunk_text = chunk_fn(row)

            # Embed
            embedding = await embedding_client.embed(chunk_text)

            # Build metadata stored alongside the vector
            metadata = {
                "org_id":    org_id,
                "doc_type":  doc_type,
                "domain":    DOMAIN,
                "period":    row.get("period"),
                "emp_id":    row.get("emp_id"),
                "location_id": row.get("location_id"),
            }

            # Upsert into pgvector
            await vector_store.upsert(
                doc_id=doc_id,
                tenant_id=str(org_id),
                doc_domain=DOMAIN,
                doc_type=doc_type,
                chunk_text=chunk_text,
                embedding=embedding,
                metadata=metadata,
            )

            created += 1
            logger.debug(
                "revenue handler: embedded doc_id=%s doc_type=%s org=%d",
                doc_id, doc_type, org_id,
            )

        except Exception as exc:
            failed += 1
            logger.error(
                "revenue handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "revenue handler done org=%d created=%d skipped=%d failed=%d",
        org_id, created, skipped, failed,
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}
