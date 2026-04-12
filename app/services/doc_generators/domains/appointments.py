"""
app/services/doc_generators/domains/appointments.py
====================================================
Appointments domain document handler for DocGenerator.

Reads the 4 appointment document types produced by the ETL extractor
from the analytics warehouse, generates human-readable chunk_text
for each one, and stores embeddings in pgvector.

Called by DocGenerator.generate_all() when domain="appointments" or domain=None.

Document types handled:
    appt_monthly_summary      → one doc per location per period (+ org rollup)
    appt_staff_breakdown      → one doc per staff member per period
    appt_service_breakdown    → one doc per service per period
    appt_staff_service_cross  → one doc per staff+service per period
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "appointments"

# Doc types this handler owns
APPOINTMENTS_DOC_TYPES = {
    "appt_monthly_summary",
    "appt_staff_breakdown",
    "appt_service_breakdown",
    "appt_staff_service_cross",
}


# ---------------------------------------------------------------------------
# chunk_text generators  (one per doc_type)
# These produce the human-readable text that gets embedded into pgvector.
# The ETL already writes a `text` field — these enrich it into a richer
# narrative chunk suitable for RAG injection.
# ---------------------------------------------------------------------------

def _chunk_monthly_summary(row: dict) -> str:
    period       = row.get("period", "unknown")
    loc_name     = row.get("location_name", "All Locations")
    loc_city     = row.get("location_city", "")
    is_rollup    = row.get("is_rollup", False) or row.get("location_id", 1) == 0

    total        = int(row.get("total_booked", 0) or 0)
    completed    = int(row.get("completed_count", 0) or 0)
    cancelled    = int(row.get("cancelled_count", 0) or 0)
    no_shows     = int(row.get("no_show_count", 0) or 0)
    cancel_rate  = float(row.get("cancellation_rate_pct", 0.0) or 0.0)
    no_show_rate = float(row.get("no_show_rate_pct", 0.0) or 0.0)
    mom          = row.get("mom_growth_pct")

    morning      = int(row.get("morning_count", 0) or 0)
    afternoon    = int(row.get("afternoon_count", 0) or 0)
    evening      = int(row.get("evening_count", 0) or 0)
    weekend      = int(row.get("weekend_count", 0) or 0)
    weekday      = int(row.get("weekday_count", 0) or 0)
    # Compute peak_slot from counts if not stored (raw fixture rows)
    _stored_peak = row.get("peak_slot")
    if _stored_peak and _stored_peak != "unknown":
        peak_slot = _stored_peak
    else:
        slot_counts = {"morning": morning, "afternoon": afternoon, "evening": evening}
        peak_slot = max(slot_counts, key=slot_counts.get) if any(slot_counts.values()) else "unknown"
    avg_dur      = row.get("avg_actual_duration_min")

    walkin       = int(row.get("walkin_count", 0) or 0)
    app_book     = int(row.get("app_booking_count", 0) or 0)

    # Location label — per-location docs must be clearly distinct from rollup
    if is_rollup:
        loc_label = "All locations combined (org-level rollup)"
        loc_context = "This is the organisation-wide total across all branches."
    elif loc_city:
        loc_label = f"{loc_name} branch ({loc_city})"
        loc_context = f"Branch: {loc_name}. Location-specific data for this branch only."
    else:
        loc_label = f"{loc_name} branch"
        loc_context = f"Branch: {loc_name}. Location-specific data for this branch only."

    # MoM narrative
    if mom is None:
        mom_str = "This is the first recorded period — no prior comparison available."
    elif mom > 0:
        mom_str = f"Branch appointment volume increased {mom:.1f}% vs the previous period."
    elif mom < 0:
        mom_str = f"Branch appointment volume decreased {abs(mom):.1f}% vs the previous period."
    else:
        mom_str = "Branch appointment volume was flat vs the previous period."

    # Duration line
    dur_str = (
        f"Average appointment duration: {avg_dur:.0f} minutes."
        if avg_dur is not None
        else ""
    )

    # Booking source line
    total_source = walkin + app_book
    if total_source > 0:
        walkin_pct   = round(walkin / total_source * 100, 1)
        app_book_pct = round(app_book / total_source * 100, 1)
        source_str = (
            f"Booking source: {walkin} walk-ins ({walkin_pct}%), "
            f"{app_book} app bookings ({app_book_pct}%)."
        )
    else:
        source_str = ""

    return (
        f"Appointment Summary — {loc_label} — {period}\n"
        f"{loc_context}\n"
        f"Total booked: {total}. Completed: {completed}. "
        f"Cancelled: {cancelled} ({cancel_rate:.1f}% cancellation rate). "
        f"No-shows: {no_shows} ({no_show_rate:.1f}%).\n"
        f"{mom_str}\n"
        f"Time slot distribution: {morning} morning, {afternoon} afternoon, "
        f"{evening} evening. Peak slot: {peak_slot}. "
        f"Weekday: {weekday}. Weekend: {weekend}.\n"
        + (f"{dur_str}\n" if dur_str else "")
        + (f"{source_str}" if source_str else "")
    ).strip()


def _chunk_staff_breakdown(row: dict) -> str:
    period          = row.get("period", "unknown")
    staff_name      = row.get("staff_name", "Unknown Staff")
    loc_name        = row.get("location_name", "Unknown Location")
    total           = int(row.get("total_booked", 0) or 0)
    completed       = int(row.get("completed_count", 0) or 0)
    cancelled       = int(row.get("cancelled_count", 0) or 0)
    no_shows        = int(row.get("no_show_count", 0) or 0)
    # Compute completion_rate from stored field or derive from raw counts
    completion_rate = float(
        row.get("completion_rate_pct")
        if row.get("completion_rate_pct") is not None
        else (round(completed / total * 100, 1) if total > 0 else 0.0)
    )
    no_show_rate    = float(row.get("no_show_rate_pct", 0.0) or 0.0)
    services        = int(row.get("distinct_services_handled", 0) or 0)
    mom             = row.get("mom_growth_pct")

    # Trend narrative
    if mom is None:
        trend_str = "First period on record — no prior comparison available."
    elif mom > 5:
        trend_str = f"Appointment bookings are growing — up {mom:.1f}% vs the previous period."
    elif mom < -5:
        trend_str = f"Appointment bookings are declining — down {abs(mom):.1f}% vs the previous period."
    else:
        trend_str = f"Appointment bookings are stable ({mom:+.1f}% vs the previous period)."

    return (
        f"Staff Appointments — {staff_name} at {loc_name} branch — {period}\n"
        f"Staff member: {staff_name}. Branch/location: {loc_name}.\n"
        f"Completed appointments at {loc_name}: {completed} ({completion_rate:.1f}% completion rate).\n"
        f"Total booked: {total}. Cancelled: {cancelled}. No-shows: {no_shows} ({no_show_rate:.1f}% no-show rate).\n"
        f"{trend_str}\n"
        f"Distinct service types handled: {services}."
    ).strip()


def _chunk_service_breakdown(row: dict) -> str:
    period        = row.get("period", "unknown")
    service_name  = row.get("service_name", "Unknown Service")
    total         = int(row.get("total_booked", 0) or 0)
    completed     = int(row.get("completed_count", 0) or 0)
    cancelled     = int(row.get("cancelled_count", 0) or 0)
    distinct      = int(row.get("distinct_clients", 0) or 0)
    repeats       = int(row.get("repeat_visit_count", 0) or 0)
    cancel_rate   = float(row.get("cancellation_rate_pct", 0.0) or 0.0)
    sched_dur     = row.get("avg_scheduled_duration_min")
    actual_dur    = row.get("avg_actual_duration_min")
    morning       = int(row.get("morning_count", 0) or 0)
    afternoon     = int(row.get("afternoon_count", 0) or 0)
    evening       = int(row.get("evening_count", 0) or 0)
    peak_slot     = row.get("peak_slot", "unknown")

    # Duration commentary
    if actual_dur is not None and sched_dur is not None:
        diff = actual_dur - sched_dur
        if diff > 5:
            dur_str = (
                f"Average actual duration: {actual_dur:.0f} min "
                f"({diff:.0f} min over the scheduled {sched_dur:.0f} min)."
            )
        elif diff < -5:
            dur_str = (
                f"Average actual duration: {actual_dur:.0f} min "
                f"({abs(diff):.0f} min under the scheduled {sched_dur:.0f} min)."
            )
        else:
            dur_str = f"Average duration: {actual_dur:.0f} min (on schedule, {sched_dur:.0f} min planned)."
    elif actual_dur is not None:
        dur_str = f"Average actual duration: {actual_dur:.0f} min."
    elif sched_dur is not None:
        dur_str = f"Scheduled duration: {sched_dur:.0f} min (no actual data)."
    else:
        dur_str = "Duration not recorded."

    # Compute peak_slot from counts if not stored
    _stored_peak = row.get("peak_slot")
    if _stored_peak and _stored_peak != "unknown":
        peak_slot = _stored_peak
    else:
        _slots = {"morning": morning, "afternoon": afternoon, "evening": evening}
        peak_slot = max(_slots, key=_slots.get) if any(_slots.values()) else "unknown"

    # Repeat client note
    if distinct > 0:
        repeat_pct = round(repeats / total * 100, 1) if total > 0 else 0
        repeat_str = (
            f"{distinct} unique clients booked this service; "
            f"{repeats} repeat visits within the period ({repeat_pct}% repeat rate)."
        )
    else:
        repeat_str = "No client breakdown available."

    # Cancellation pattern note — explicit signal for Q26
    if cancel_rate >= 15:
        cancel_signal = f"High cancellation rate alert: {cancel_rate:.1f}% — above normal threshold."
    elif cancel_rate >= 10:
        cancel_signal = f"Elevated cancellation rate: {cancel_rate:.1f}%."
    else:
        cancel_signal = f"Cancellation rate: {cancel_rate:.1f}% (within normal range)."

    # Booking frequency note — explicit signal for Q22
    booking_signal = (
        f"Booking frequency: {total} appointments in this period. "
        f"Completion: {completed} completed ({round(completed/total*100,1) if total else 0}%)."
    )

    return (
        f"Service Appointments — {service_name} — {period}\n"
        f"Service: {service_name}. Booking frequency this period: {total} appointments.\n"
        f"{booking_signal}\n"
        f"{cancel_signal}\n"
        f"{repeat_str}\n"
        f"{dur_str}\n"
        f"Time slot distribution: {morning} morning, {afternoon} afternoon, "
        f"{evening} evening. Peak booking slot: {peak_slot}."
    ).strip()


def _chunk_staff_service_cross(row: dict) -> str:
    period          = row.get("period", "unknown")
    staff_name      = row.get("staff_name", "Unknown Staff")
    service_name    = row.get("service_name", "Unknown Service")
    total           = int(row.get("total_booked", 0) or 0)
    completed       = int(row.get("completed_count", 0) or 0)
    completion_rate = float(
        row.get("completion_rate_pct")
        if row.get("completion_rate_pct") is not None
        else (round(completed / total * 100, 1) if total > 0 else 0.0)
    )

    return (
        f"Staff-Service Appointments — {staff_name} / {service_name} — {period}\n"
        f"{staff_name} handled {total} {service_name} appointments, "
        f"completing {completed} ({completion_rate:.1f}% completion rate)."
    ).strip()


CHUNK_GENERATORS = {
    "appt_monthly_summary":    _chunk_monthly_summary,
    "appt_staff_breakdown":    _chunk_staff_breakdown,
    "appt_service_breakdown":  _chunk_service_breakdown,
    "appt_staff_service_cross": _chunk_staff_service_cross,
}


# ---------------------------------------------------------------------------
# doc_id — stable, content-addressable
# ---------------------------------------------------------------------------

def _make_doc_id(org_id: int, doc_type: str, row: dict) -> str:
    """
    Deterministic doc_id so we can skip re-embedding unchanged documents
    and upsert correctly on re-runs.

    Format: appointments:{org_id}:{doc_type}:{discriminator_hash}
    """
    parts = [str(org_id), doc_type]

    if "period" in row:
        parts.append(str(row["period"]))
    if "location_id" in row:
        parts.append(str(row["location_id"]))
    if "staff_id" in row:
        parts.append(str(row["staff_id"]))
    if "service_id" in row:
        parts.append(str(row["service_id"]))

    base = ":".join(parts)
    h = hashlib.sha256(base.encode()).hexdigest()[:12]
    return f"appointments:{org_id}:{doc_type}:{h}"


# ---------------------------------------------------------------------------
# Main handler — called by DocGenerator
# ---------------------------------------------------------------------------

async def generate_appointments_docs(
    org_id: int,
    warehouse_rows: list[dict],
    embedding_client: Any,
    vector_store: Any,
    force: bool = False,
) -> dict[str, int]:
    """
    Generate and embed all appointment documents for one org.

    Parameters
    ----------
    org_id:
        The tenant / business ID.
    warehouse_rows:
        Documents produced by AppointmentsExtractor.run() and stored in
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

        if doc_type not in APPOINTMENTS_DOC_TYPES:
            logger.debug(
                "appointments handler: skipping unknown doc_type=%s", doc_type
            )
            continue

        doc_id = _make_doc_id(org_id, doc_type, row)

        # Skip if already embedded (unless --force)
        if not force and await vector_store.exists(str(org_id), doc_id):
            skipped += 1
            continue

        chunk_fn = CHUNK_GENERATORS.get(doc_type)
        if chunk_fn is None:
            logger.warning(
                "appointments handler: no chunk generator for doc_type=%s", doc_type
            )
            failed += 1
            continue

        try:
            chunk_text = chunk_fn(row)

            # Embed
            embedding = await embedding_client.embed(chunk_text)

            # Metadata stored alongside the vector
            metadata = {
                "org_id":      org_id,
                "doc_type":    doc_type,
                "domain":      DOMAIN,
                "period":      row.get("period"),
                "location_id": row.get("location_id"),
                "staff_id":    row.get("staff_id"),
                "service_id":  row.get("service_id"),
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
                "appointments handler: embedded doc_id=%s doc_type=%s org=%d",
                doc_id, doc_type, org_id,
            )

        except Exception as exc:
            failed += 1
            logger.error(
                "appointments handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "appointments handler done org=%d created=%d skipped=%d failed=%d",
        org_id, created, skipped, failed,
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}