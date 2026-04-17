"""
app/services/doc_generators/domains/services.py
=================================================
Services domain document generator.

Converts warehouse rows from the 5 wh_svc_* tables into natural-language
chunk text suitable for embedding in pgvector. Each chunk is designed to
be self-contained — the LLM can answer a question from a single retrieved
chunk without needing to cross-reference other chunks.

Doc types produced:
    svc_monthly_summary    — revenue, margin, clients per service per location per month
    svc_booking_stats      — bookings, cancellations, duration, time slots per service
    svc_staff_matrix       — who performs which services, volume, revenue
    svc_co_occurrence      — services commonly performed together
    svc_catalog            — catalog snapshot with lifecycle signals

Usage:
    docs = generate_service_docs(org_id=42, data=extractor_result)
    # docs → list of dicts, each with 'doc_id', 'doc_domain', 'doc_type',
    #         'chunk_text', 'period_start', 'metadata'
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Chunk text generators — one per doc type
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_monthly_summary(row: dict) -> str:
    svc    = row.get("service_name", "Unknown Service")
    cat    = row.get("category_name") or "Uncategorized"
    loc    = row.get("location_name", "Unknown")
    period = row.get("period_start", "unknown")
    perf   = int(row.get("performed_count", 0) or 0)
    clients = int(row.get("distinct_clients", 0) or 0)
    repeat = int(row.get("repeat_visit_proxy", 0) or 0)
    rev    = float(row.get("total_revenue", 0) or 0)
    avg_p  = float(row.get("avg_charged_price", 0) or 0)
    comm   = float(row.get("total_emp_commission", 0) or 0)
    margin = float(row.get("gross_margin", 0) or 0)
    cpct   = row.get("commission_pct_of_revenue")
    mom    = row.get("mom_revenue_growth_pct")
    rev_rk = row.get("revenue_rank")
    mar_rk = row.get("margin_rank")

    rev_per_appt = round(rev / perf, 2) if perf > 0 else 0
    repeat_pct = round(repeat / perf * 100, 1) if perf > 0 else 0

    lines = [
        f"Service Performance — {svc} ({cat}) — {loc} — {period}",
        f"Performed {perf} times for {clients} unique clients "
        f"({repeat} repeat visits, {repeat_pct}% repeat rate).",
        f"Revenue: ${rev:,.2f} (avg ${avg_p:.2f} per appointment, "
        f"${rev_per_appt:.2f} revenue per visit).",
        f"Commission cost: ${comm:,.2f} "
        f"({cpct:.1f}% of revenue)." if cpct is not None
        else f"Commission cost: ${comm:,.2f}.",
        f"Gross margin: ${margin:,.2f}.",
    ]

    if rev_rk is not None:
        lines.append(f"Revenue rank this period: #{rev_rk}.")
    if mar_rk is not None:
        lines.append(f"Margin rank this period: #{mar_rk}.")
    if mom is not None:
        direction = "up" if mom > 0 else "down" if mom < 0 else "flat"
        lines.append(f"Month-over-month revenue change: {mom:+.1f}% ({direction}).")

    return "\n".join(lines).strip()


def _chunk_booking_stats(row: dict) -> str:
    svc      = row.get("service_name", "Unknown Service")
    loc      = row.get("location_name", "Unknown")
    period   = row.get("period_start", "unknown")
    booked   = int(row.get("total_booked", 0) or 0)
    completed = int(row.get("completed_count", 0) or 0)
    cancelled = int(row.get("cancelled_count", 0) or 0)
    no_show  = int(row.get("no_show_count", 0) or 0)
    canc_pct = row.get("cancellation_rate_pct", 0)
    dur      = row.get("avg_actual_duration_min")
    clients  = int(row.get("distinct_clients", 0) or 0)
    morning  = int(row.get("morning_bookings", 0) or 0)
    afternoon = int(row.get("afternoon_bookings", 0) or 0)
    evening  = int(row.get("evening_bookings", 0) or 0)
    mom      = row.get("mom_bookings_growth_pct")

    slots = {"morning": morning, "afternoon": afternoon, "evening": evening}
    peak = max(slots, key=slots.get) if any(slots.values()) else "n/a"

    lines = [
        f"Service Bookings — {svc} — {loc} — {period}",
        f"Total booked: {booked}. Completed: {completed}. "
        f"Cancelled: {cancelled} ({canc_pct:.1f}%). No-shows: {no_show}.",
        f"{clients} unique clients booked this service.",
    ]

    if dur is not None:
        lines.append(f"Average actual duration: {dur:.1f} minutes.")

    lines.append(
        f"Time slots: {morning} morning, {afternoon} afternoon, "
        f"{evening} evening. Peak: {peak}."
    )

    if mom is not None:
        direction = "up" if mom > 0 else "down" if mom < 0 else "flat"
        lines.append(f"Month-over-month bookings change: {mom:+.1f}% ({direction}).")

    return "\n".join(lines).strip()


def _chunk_staff_matrix(row: dict) -> str:
    svc    = row.get("service_name", "Unknown Service")
    staff  = row.get("staff_name", "Unknown Staff")
    period = row.get("period_start", "unknown")
    perf   = int(row.get("performed_count", 0) or 0)
    rev    = float(row.get("revenue", 0) or 0)
    comm   = float(row.get("commission_paid", 0) or 0)

    return (
        f"Staff-Service — {staff} / {svc} — {period}\n"
        f"{staff} performed {svc} {perf} times, "
        f"generating ${rev:,.2f} in revenue "
        f"(${comm:,.2f} commission paid)."
    ).strip()


def _chunk_co_occurrence(row: dict) -> str:
    period   = row.get("period_start", "unknown")
    svc_a    = row.get("service_a_name", "Service A")
    svc_b    = row.get("service_b_name", "Service B")
    count    = int(row.get("co_occurrence_count", 0) or 0)

    return (
        f"Service Pairing — {svc_a} + {svc_b} — {period}\n"
        f"{svc_a} and {svc_b} were performed together in the same visit "
        f"{count} times this period. These services are commonly booked "
        f"as a combo by clients."
    ).strip()


def _chunk_catalog(row: dict) -> str:
    svc      = row.get("service_name", "Unknown Service")
    cat      = row.get("category_name") or "Uncategorized"
    price    = float(row.get("list_price", 0) or 0)
    duration = int(row.get("scheduled_duration_min", 0) or 0)
    active   = row.get("is_active", True)
    dormant  = row.get("dormant_flag", False)
    new      = row.get("is_new_this_year", False)
    discount = row.get("avg_discount_pct")
    delta    = row.get("scheduled_vs_actual_delta_min")
    last     = row.get("last_sold_date")
    days     = row.get("days_since_last_sale")
    lifetime = int(row.get("lifetime_performed_count", 0) or 0)
    first_ct = int(row.get("new_client_first_service_count", 0) or 0)
    comm_rate = row.get("default_commission_rate")
    comm_type = row.get("commission_type", "%")
    loc_id   = row.get("home_location_id")

    status_parts = []
    if not active:
        status_parts.append("INACTIVE (discontinued)")
    elif dormant:
        status_parts.append(f"DORMANT — no sales in {days} days" if days else "DORMANT")
    else:
        status_parts.append("Active")
    if new:
        status_parts.append("NEW this year")
    status = ", ".join(status_parts)

    lines = [
        f"Service Catalog — {svc} ({cat}) — Status: {status}",
        f"List price: ${price:.2f}. Scheduled duration: {duration} minutes.",
    ]

    if comm_rate is not None:
        lines.append(f"Default commission: {comm_rate}{comm_type}.")

    if loc_id is not None:
        lines.append(f"Available at location ID {loc_id} only.")
    else:
        lines.append("Available at all locations.")

    lines.append(f"Lifetime performed: {lifetime} times.")
    lines.append(f"First service for {first_ct} new clients.")

    if discount is not None:
        lines.append(f"Average discount from list price: {discount:.1f}%.")

    if delta is not None:
        if delta > 2:
            lines.append(
                f"Runs {delta:.1f} minutes over scheduled duration on average "
                f"— consider adjusting schedule."
            )
        elif delta < -2:
            lines.append(
                f"Finishes {abs(delta):.1f} minutes under scheduled duration."
            )
        else:
            lines.append(f"Duration within {abs(delta):.1f} minutes of schedule.")

    if last:
        lines.append(f"Last sold: {last[:10]}. Days since last sale: {days}.")

    return "\n".join(lines).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Chunk generator registry
# ─────────────────────────────────────────────────────────────────────────────

CHUNK_GENERATORS = {
    "svc_monthly_summary":  _chunk_monthly_summary,
    "svc_booking_stats":    _chunk_booking_stats,
    "svc_staff_matrix":     _chunk_staff_matrix,
    "svc_co_occurrence":    _chunk_co_occurrence,
    "svc_catalog":          _chunk_catalog,
}


# ─────────────────────────────────────────────────────────────────────────────
# doc_id — stable, content-addressable
# ─────────────────────────────────────────────────────────────────────────────

def _make_doc_id(org_id: int, doc_type: str, row: dict) -> str:
    """
    Deterministic doc_id for upsert into pgvector.
    Same inputs always produce the same ID → safe to re-embed.
    """
    parts = [str(org_id), doc_type]

    if doc_type == "svc_monthly_summary":
        parts += [str(row.get("service_id", "")), str(row.get("location_id", "")),
                  str(row.get("period_start", ""))]
    elif doc_type == "svc_booking_stats":
        parts += [str(row.get("service_id", "")), str(row.get("location_id", "")),
                  str(row.get("period_start", ""))]
    elif doc_type == "svc_staff_matrix":
        parts += [str(row.get("service_id", "")), str(row.get("staff_id", "")),
                  str(row.get("period_start", ""))]
    elif doc_type == "svc_co_occurrence":
        parts += [str(row.get("service_a_id", "")), str(row.get("service_b_id", "")),
                  str(row.get("period_start", ""))]
    elif doc_type == "svc_catalog":
        parts += [str(row.get("service_id", ""))]
    else:
        parts.append(hashlib.md5(str(row).encode()).hexdigest()[:8])

    return ":".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Main generator entry point
# ─────────────────────────────────────────────────────────────────────────────

def generate_service_docs(org_id: int, data: dict) -> dict:
    """
    Convert extractor output into embedding-ready documents.

    Parameters
    ----------
    org_id : int — tenant ID
    data   : dict — output from ServicesExtractor.run()

    Returns
    -------
    dict with keys:
        docs_created : int
        docs_skipped : int
        docs_failed  : int
        docs         : list[dict] — each dict has:
            doc_id, tenant_id, doc_domain, doc_type,
            chunk_text, period_start, metadata
    """
    DOC_TYPE_MAP = {
        "monthly_summary": "svc_monthly_summary",
        "booking_stats":   "svc_booking_stats",
        "staff_matrix":    "svc_staff_matrix",
        "co_occurrence":   "svc_co_occurrence",
        "catalog":         "svc_catalog",
    }

    docs = []
    created = 0
    skipped = 0
    failed = 0

    for data_key, doc_type in DOC_TYPE_MAP.items():
        rows = data.get(data_key, [])
        chunk_fn = CHUNK_GENERATORS[doc_type]

        for row in rows:
            try:
                chunk_text = chunk_fn(row)
                if not chunk_text or len(chunk_text) < 20:
                    skipped += 1
                    continue

                doc_id = _make_doc_id(org_id, doc_type, row)

                # Period start — convert string to date for asyncpg
                raw_period = row.get("period_start")
                if raw_period and isinstance(raw_period, str):
                    parts = raw_period.split("-")
                    if len(parts) == 2:
                        period = date(int(parts[0]), int(parts[1]), 1)
                    elif len(parts) == 3:
                        period = date(int(parts[0]), int(parts[1]), int(parts[2]))
                    else:
                        period = None
                else:
                    period = raw_period

                doc = {
                    "doc_id":       doc_id,
                    "tenant_id":    str(org_id),
                    "doc_domain":   "services",
                    "doc_type":     doc_type,
                    "chunk_text":   chunk_text,
                    "period_start": period,
                    "metadata": {
                        "service_id":   row.get("service_id"),
                        "service_name": row.get("service_name"),
                        "location_id":  row.get("location_id"),
                        "staff_id":     row.get("staff_id"),
                        "category":     row.get("category_name"),
                    },
                }
                docs.append(doc)
                created += 1

            except Exception as exc:
                logger.warning(
                    "Failed to generate doc for %s row: %r",
                    doc_type, exc,
                )
                failed += 1

    logger.info(
        "generate_service_docs — created=%d skipped=%d failed=%d",
        created, skipped, failed,
    )

    return {
        "docs_created": created,
        "docs_skipped": skipped,
        "docs_failed":  failed,
        "docs":         docs,
    }