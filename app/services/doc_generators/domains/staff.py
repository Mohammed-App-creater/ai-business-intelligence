"""
app/services/doc_generators/domains/staff.py
=============================================
Staff Performance domain document handler for DocGenerator.

Reads the 3 staff document types produced by the ETL extractor
from the analytics warehouse, generates human-readable chunk_text
for each one, and stores embeddings in pgvector.

Called by DocGenerator.generate_all() when domain="staff" or domain=None.

Document types handled:
    staff_monthly     → one doc per staff × location × period (Q1 monthly data)
    staff_summary     → one doc per staff (all-time YTD totals)
    staff_attendance  → one doc per staff × location × period (hours worked)

NOTE ON DOMAIN BOUNDARIES:
    No-show rates, cancellation rates, and completion rates per staff
    are handled by the appointments domain (doc_type='appt_staff_breakdown').
    This handler does NOT duplicate those fields.
    Q38/Q39/Q40 are answered by the AI layer combining both domains.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "staff"

STAFF_DOC_TYPES = {
    "staff_monthly",
    "staff_summary",
    "staff_attendance",
}


# ---------------------------------------------------------------------------
# chunk_text generators — one per doc_type
# These produce the human-readable text that gets embedded into pgvector.
# Vocabulary mirrors the 40 test questions so semantic search finds them.
# ---------------------------------------------------------------------------

def _chunk_staff_monthly(row: dict) -> str:
    """
    Monthly KPI chunk for one staff member at one location in one period.
    Powers Q1–Q8, Q11–Q22, Q25–Q32, Q34–Q35, Q37–Q38.
    """
    name          = row.get("staff_full_name", "Unknown Staff")
    first         = row.get("staff_first_name", name.split()[0] if name else "")
    loc_name      = row.get("location_name", "Unknown Location")
    period        = row.get("period_label", "unknown")
    is_active     = row.get("is_active", True)
    hire_date     = row.get("hire_date", "")

    visits        = int(row.get("completed_visit_count", 0) or 0)
    customers     = int(row.get("unique_customer_count", 0) or 0)
    revenue       = float(row.get("revenue", 0) or 0)
    tips          = float(row.get("tips", 0) or 0)
    total_pay     = float(row.get("total_pay", 0) or 0)
    avg_rev       = float(row.get("avg_revenue_per_visit", 0) or 0)
    commission    = float(row.get("commission_earned", 0) or 0)

    cancelled     = int(row.get("cancelled_payment_count", 0) or 0)
    refunded      = int(row.get("refunded_payment_count", 0) or 0)
    revoked       = int(row.get("revoked_payment_count", 0) or 0)
    total_bad     = cancelled + refunded + revoked

    review_count  = int(row.get("review_count", 0) or 0)
    avg_rating    = row.get("avg_rating")  # may be None

    # Active / inactive label — Q5, Q21 test cases
    status_str = (
        f"{name} is currently an active staff member."
        if is_active
        else f"{name} is no longer active (deactivated). Historical data shown."
    )

    # Rating line — Q3, Q8, Q10 — None means "no reviews yet", not zero
    if avg_rating is not None and review_count > 0:
        rating_str = (
            f"Customer rating: {avg_rating:.1f}/5.0 "
            f"based on {review_count} review{'s' if review_count != 1 else ''}."
        )
    elif review_count == 0:
        rating_str = "Customer rating: no reviews recorded this period."
    else:
        rating_str = f"Customer rating: {review_count} reviews recorded."

    # Commission — Q34, Q37
    commission_str = (
        f"Commission earned: ${commission:,.2f}."
        if commission > 0
        else "Commission: none recorded this period."
    )

    # Cancelled / refunded — Q24, Q38, Q39 pointer
    bad_payment_str = ""
    if total_bad > 0:
        parts = []
        if cancelled: parts.append(f"{cancelled} cancelled")
        if refunded:  parts.append(f"{refunded} refunded")
        if revoked:   parts.append(f"{revoked} revoked")
        bad_payment_str = (
            f"Payment-level issues: {', '.join(parts)} "
            f"(these are payment cancellations, not appointment no-shows — "
            f"see appointment domain for no-show data)."
        )

    # Hire date — Q22 (staff who joined this year)
    hire_str = f"Hire date: {hire_date}." if hire_date else ""

    return (
        f"Staff Performance — {name} — {loc_name} — {period}\n"
        f"Staff member: {name} (also referred to as {first}, "
        f"employee, stylist, technician, worker, team member).\n"
        f"Location / branch: {loc_name}.\n"
        f"{status_str}\n"
        f"{hire_str}\n"
        f"Completed appointments (visits): {visits}. "
        f"Unique customers served: {customers}.\n"
        f"Revenue generated: ${revenue:,.2f}. "
        f"Average revenue per visit: ${avg_rev:,.2f}. "
        f"Tips collected: ${tips:,.2f}. "
        f"Total collected (revenue + tips): ${total_pay:,.2f}.\n"
        f"{commission_str}\n"
        f"{rating_str}\n"
        + (f"{bad_payment_str}\n" if bad_payment_str else "")
    ).strip()


def _chunk_staff_summary(row: dict) -> str:
    """
    All-time / YTD summary chunk for one staff member.
    Powers Q9 (rank by revenue), Q10 (lowest rating), Q29, Q31.
    """
    name          = row.get("staff_full_name", "Unknown Staff")
    first         = row.get("staff_first_name", "")
    is_active     = row.get("is_active", True)
    hire_date     = row.get("hire_date", "")

    total_visits  = int(row.get("total_visits_ytd", 0) or 0)
    total_rev     = float(row.get("total_revenue_ytd", 0) or 0)
    total_tips    = float(row.get("total_tips_ytd", 0) or 0)
    total_comm    = float(row.get("total_commission_ytd", 0) or 0)
    customers     = int(row.get("total_customers_served", 0) or 0)
    total_cancel  = int(row.get("total_cancelled_ytd", 0) or 0)
    avg_rating    = row.get("overall_avg_rating")
    review_count  = int(row.get("total_review_count", 0) or 0)
    avg_rev_visit = float(row.get("lifetime_avg_revenue_per_visit", 0) or 0)
    rev_pct       = row.get("revenue_pct_of_org_latest")  # None for inactive

    first_period  = row.get("first_active_period", "")
    last_period   = row.get("last_active_period", "")

    status_str = (
        f"{name} is currently an active team member."
        if is_active
        else f"{name} is no longer active (deactivated). Summary of historical contribution."
    )

    rating_str = (
        f"Overall customer rating: {avg_rating:.1f}/5.0 ({review_count} total reviews)."
        if avg_rating is not None
        else f"Customer rating: {review_count} reviews on record (no average available)."
        if review_count > 0
        else "Customer rating: no reviews on record."
    )

    rev_pct_str = (
        f"Revenue share in most recent period: {rev_pct:.1f}% of total business revenue."
        if rev_pct is not None
        else "Revenue share: not available (inactive in most recent period)."
    )

    tenure_str = (
        f"Active from {first_period} to {last_period}."
        if first_period and last_period
        else ""
    )
    hire_str = f"Hire date: {hire_date}." if hire_date else ""

    return (
        f"Staff Summary (All-Time) — {name}\n"
        f"Staff member: {name} (also referred to as {first}, "
        f"employee, stylist, technician, worker, team member, top performer, MVP).\n"
        f"{status_str}\n"
        f"{hire_str} {tenure_str}\n"
        f"Total visits completed (YTD): {total_visits}. "
        f"Total customers served: {customers}.\n"
        f"Total revenue generated (YTD): ${total_rev:,.2f}. "
        f"Average revenue per visit: ${avg_rev_visit:,.2f}.\n"
        f"Total tips (YTD): ${total_tips:,.2f}. "
        f"Total commission earned (YTD): ${total_comm:,.2f}.\n"
        f"Total cancelled payments (YTD): {total_cancel}.\n"
        f"{rating_str}\n"
        f"{rev_pct_str}"
    ).strip()


def _chunk_staff_attendance(row: dict) -> str:
    """
    Monthly attendance hours chunk for one staff member at one location.
    Powers Q33 (who clocked the most hours).
    """
    name            = row.get("staff_full_name", "Unknown Staff")
    first           = row.get("staff_first_name", "")
    loc_name        = row.get("location_name", "Unknown Location")
    period          = row.get("period_label", "unknown")
    is_active       = row.get("is_active", True)

    days_signin     = int(row.get("days_with_signin", 0) or 0)
    days_full       = int(row.get("days_fully_recorded", 0) or 0)
    days_missing    = int(row.get("days_missing_signout", 0) or 0)
    total_hours     = float(row.get("total_hours_worked", 0) or 0)
    avg_hours       = row.get("avg_hours_per_day")  # None when days_full = 0

    status_str = "(inactive)" if not is_active else ""

    # Data quality note — documents the dev DB artifact we found
    quality_str = ""
    if days_missing > 0:
        quality_str = (
            f"Note: {days_missing} day{'s' if days_missing > 1 else ''} "
            f"had sign-in recorded but no sign-out — those days are excluded "
            f"from the hours calculation."
        )

    avg_str = (
        f"Average hours per working day: {avg_hours:.1f}h."
        if avg_hours is not None
        else "Average hours per day: not available (no fully recorded days)."
    )

    return (
        f"Staff Attendance — {name} — {loc_name} — {period}\n"
        f"Staff member: {name} {status_str} "
        f"(also referred to as {first}, employee, worker, team member).\n"
        f"Location / branch: {loc_name}.\n"
        f"Days signed in: {days_signin}. "
        f"Days fully recorded (sign-in and sign-out): {days_full}.\n"
        f"Total hours worked (clocked): {total_hours:.1f} hours.\n"
        f"{avg_str}\n"
        + (f"{quality_str}" if quality_str else "")
    ).strip()


CHUNK_GENERATORS = {
    "staff_monthly":    _chunk_staff_monthly,
    "staff_summary":    _chunk_staff_summary,
    "staff_attendance": _chunk_staff_attendance,
}


# ---------------------------------------------------------------------------
# doc_id — stable, content-addressable (same pattern as appointments)
# ---------------------------------------------------------------------------

def _make_doc_id(org_id: int, doc_type: str, row: dict) -> str:
    """
    Deterministic doc_id so we can skip re-embedding unchanged documents
    and upsert correctly on re-runs.

    Format: staff:{org_id}:{doc_type}:{discriminator_hash}
    """
    parts = [str(org_id), doc_type]

    if "staff_id" in row:
        parts.append(str(row["staff_id"]))
    if "period_label" in row:
        parts.append(str(row["period_label"]))
    if "location_id" in row:
        parts.append(str(row["location_id"]))

    base = ":".join(parts)
    h = hashlib.sha256(base.encode()).hexdigest()[:12]
    return f"staff:{org_id}:{doc_type}:{h}"


# ---------------------------------------------------------------------------
# Main handler — called by DocGenerator (same signature as appointments)
# ---------------------------------------------------------------------------

async def generate_staff_docs(
    org_id: int,
    warehouse_rows: list[dict],
    embedding_client: Any,
    vector_store: Any,
    force: bool = False,
) -> dict[str, int]:
    """
    Generate and embed all staff documents for one org.

    Parameters
    ----------
    org_id:
        The tenant / business ID.
    warehouse_rows:
        Documents produced by StaffExtractor.run() and stored in
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

        if doc_type not in STAFF_DOC_TYPES:
            logger.debug(
                "staff handler: skipping unknown doc_type=%s", doc_type
            )
            continue

        doc_id = _make_doc_id(org_id, doc_type, row)

        if not force and await vector_store.exists(str(org_id), doc_id):
            skipped += 1
            continue

        chunk_fn = CHUNK_GENERATORS.get(doc_type)
        if chunk_fn is None:
            logger.warning(
                "staff handler: no chunk generator for doc_type=%s", doc_type
            )
            failed += 1
            continue

        try:
            chunk_text = chunk_fn(row)

            embedding = await embedding_client.embed(chunk_text)

            metadata = {
                "org_id":      org_id,
                "doc_type":    doc_type,
                "domain":      DOMAIN,
                "staff_id":    row.get("staff_id"),
                "period":      row.get("period_label"),
                "location_id": row.get("location_id"),
                "is_active":   row.get("is_active"),
            }

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
                "staff handler: embedded doc_id=%s doc_type=%s org=%d",
                doc_id, doc_type, org_id,
            )

        except Exception as exc:
            failed += 1
            logger.error(
                "staff handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "staff handler done org=%d created=%d skipped=%d failed=%d",
        org_id, created, skipped, failed,
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}