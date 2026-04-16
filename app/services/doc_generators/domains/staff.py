"""
app/services/doc_generators/domains/staff.py
============================================
Staff domain document handler for DocGenerator.

Reads warehouse rows produced by StaffExtractor.run() and embeds them.
Synthesizes an additional doc type (staff_rollup) from the staff_monthly
subset — no new ETL or warehouse table required.

Doc types produced (4):
  1. staff_monthly      — Per (staff × location × period) full performance row
  2. staff_summary      — Per staff YTD/all-time aggregation
  3. staff_attendance   — Per (staff × location × period) hours/days worked
  4. staff_rollup       — Per period org-wide tiered ranking (synthesized)

Chunk text design:
  - Title carries (DEACTIVATED) tag when is_active=False so cosine similarity
    ranks these docs high for "show me my deactivated staff" queries.
  - Zero-visit chunks for deactivated staff use a compressed format that
    explains *why* the metrics are zero, instead of a wall of "$0.00" lines.
  - Inactive staff chunks include extra synonyms (former, inactive, deactivated,
    left, departed) for vocabulary-variant queries.
  - The rollup chunk solves the top_k scaling problem: aggregate questions
    ("who's my top performer", "rank all staff") get answered from a single
    pre-ranked doc instead of cosine-comparing N individual docs.
    Fixed-size: top 5 of top/mid tiers, BOTTOM 5 of low tier — chunk size
    stays bounded regardless of org headcount.

Function signature for generate_staff_docs() matches the contract used by
DocGenerator._gen_staff() in app/services/doc_generators/__init__.py:
    generate_staff_docs(org_id, warehouse_rows, embedding_client, vector_store, force)
"""

from __future__ import annotations

import hashlib
import logging
import math
from collections import defaultdict
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "staff"

STAFF_DOC_TYPES = {
    "staff_monthly",
    "staff_summary",
    "staff_attendance",
    "staff_rollup",
}

# Synonym lists — embedded in chunk text so cosine similarity matches
# vocabulary-variant questions (Q25–Q33).
_SYNONYMS_ACTIVE   = "employee, stylist, technician, worker, team member"
_SYNONYMS_INACTIVE = (
    "employee, stylist, technician, worker, team member, "
    "former employee, deactivated, inactive, departed, left the team, "
    "no longer with us"
)

# Rollup tuning — fixed N per tier so chunk size stays bounded for any org size
_ROLLUP_N_PER_TIER = 5


def _money(v: float | int | None) -> str:
    """Format a number as a USD string. None → '$0.00'."""
    return f"${float(v or 0):,.2f}"


def _doc_id_hash(*parts: str) -> str:
    """12-char stable hash for use in doc_id suffixes."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _parse_period_start(period_label: str | None) -> date | None:
    """Parse 'YYYY-MM' → date(YYYY, MM, 1). Returns None for malformed input."""
    if not period_label or len(period_label) != 7:
        return None
    try:
        y, m = period_label.split("-")
        return date(int(y), int(m), 1)
    except (ValueError, AttributeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK 1 — staff_monthly
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_staff_monthly(row: dict[str, Any]) -> str:
    """Render a single (staff × location × period) row as a chunk."""
    name        = row.get("staff_full_name") or "Unknown staff"
    location    = row.get("location_name") or "Unknown location"
    period      = row.get("period_label") or "Unknown period"
    is_active   = bool(row.get("is_active", True))
    hire_date   = row.get("hire_date")
    visits      = int(row.get("completed_visit_count") or 0)
    revenue     = float(row.get("revenue") or 0.0)
    tips        = float(row.get("tips") or 0.0)
    total_pay   = float(row.get("total_pay") or 0.0)
    avg_rev     = float(row.get("avg_revenue_per_visit") or 0.0)
    commission  = float(row.get("commission_earned") or 0.0)
    cancelled   = int(row.get("cancelled_payment_count") or 0)
    refunded    = int(row.get("refunded_payment_count") or 0)
    customers   = int(row.get("unique_customer_count") or 0)
    review_n    = int(row.get("review_count") or 0)
    avg_rating  = row.get("avg_rating")

    title_status = " (DEACTIVATED)" if not is_active else ""
    title = f"Staff Performance — {name}{title_status} — {location} — {period}"

    synonyms = _SYNONYMS_INACTIVE if not is_active else _SYNONYMS_ACTIVE
    intro = (
        f"Staff member: {name} (also referred to as "
        f"{', '.join(synonyms.split(', '))})."
    )

    location_line = f"Location / branch: {location}."

    if is_active:
        status_line = f"{name} is currently an active staff member."
    else:
        status_line = (
            f"{name} is no longer active — DEACTIVATED. Historical performance "
            f"data is shown for reference. {name} is a former employee who has "
            f"left the team."
        )

    hire_line = f"Hire date: {hire_date}." if hire_date else None

    # Compressed format: deactivated + zero visits (Tom 2026 case)
    if not is_active and visits == 0:
        body_lines = [
            f"No activity in this period. {name} was deactivated and has "
            f"no completed visits, no revenue, no tips, no commission, "
            f"and no customer reviews recorded for {period}.",
            "For this staff member's historical performance, see earlier "
            "periods when they were active.",
        ]
    else:
        visits_line = (
            f"Completed appointments (visits): {visits}. "
            f"Unique customers served: {customers}."
        )
        revenue_line = (
            f"Revenue generated: {_money(revenue)}. "
            f"Average revenue per visit: {_money(avg_rev)}. "
            f"Tips collected: {_money(tips)}. "
            f"Total collected (revenue + tips): {_money(total_pay)}."
        )
        commission_line = (
            f"Commission earned: {_money(commission)}."
            if commission > 0
            else "Commission: none recorded this period."
        )
        if avg_rating is not None and review_n > 0:
            rating_line = (
                f"Customer rating: {float(avg_rating):.1f}/5.0 "
                f"based on {review_n} reviews."
            )
        else:
            rating_line = "Customer rating: no reviews recorded this period."
        cancel_line = (
            f"Payment-level issues: {cancelled} cancelled, {refunded} refunded "
            f"(these are payment cancellations, not appointment no-shows — see "
            f"appointment domain for no-show data)."
        )
        body_lines = [
            visits_line, revenue_line, commission_line, rating_line, cancel_line,
        ]

    parts = [title, intro, location_line, status_line]
    if hire_line:
        parts.append(hire_line)
    parts.extend(body_lines)
    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK 2 — staff_summary
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_staff_summary(row: dict[str, Any]) -> str:
    """Render the per-staff YTD summary as a chunk."""
    name           = row.get("staff_full_name") or "Unknown staff"
    is_active      = bool(row.get("is_active", True))
    hire_date      = row.get("hire_date")
    visits_ytd     = int(row.get("total_visits_ytd") or 0)
    rev_ytd        = float(row.get("total_revenue_ytd") or 0.0)
    tips_ytd       = float(row.get("total_tips_ytd") or 0.0)
    comm_ytd       = float(row.get("total_commission_ytd") or 0.0)
    cust_ytd       = int(row.get("total_customers_served") or 0)
    cancelled_ytd  = int(row.get("total_cancelled_ytd") or 0)
    refunded_ytd   = int(row.get("total_refunded_ytd") or 0)
    avg_rating     = row.get("overall_avg_rating")
    review_n       = int(row.get("total_review_count") or 0)
    avg_per_visit  = float(row.get("lifetime_avg_revenue_per_visit") or 0.0)
    first_active   = row.get("first_active_period") or "unknown"
    last_active    = row.get("last_active_period") or "unknown"
    rev_share      = row.get("revenue_pct_of_org_latest")

    title_status = " (DEACTIVATED)" if not is_active else ""
    title = f"Staff Performance Summary — {name}{title_status} (all-time / year-to-date)"

    synonyms = _SYNONYMS_INACTIVE if not is_active else _SYNONYMS_ACTIVE
    intro = (
        f"Staff member: {name} (also referred to as "
        f"{', '.join(synonyms.split(', '))})."
    )

    if is_active:
        status_line = (
            f"{name} is currently an active staff member. "
            f"First active period: {first_active}. Last active period: {last_active}."
        )
    else:
        status_line = (
            f"{name} is no longer active — DEACTIVATED. {name} is a former "
            f"employee who has left the team. "
            f"First active period: {first_active}. "
            f"Last period of real activity: {last_active}."
        )

    hire_line = f"Hire date: {hire_date}." if hire_date else None

    visits_line = (
        f"Total completed visits (year-to-date or all-time): {visits_ytd}. "
        f"Unique customers served: {cust_ytd}."
    )
    revenue_line = (
        f"Total revenue generated: {_money(rev_ytd)}. "
        f"Total tips collected: {_money(tips_ytd)}. "
        f"Lifetime average revenue per visit: {_money(avg_per_visit)}."
    )
    commission_line = f"Total commission earned: {_money(comm_ytd)}."

    if avg_rating is not None and review_n > 0:
        rating_line = (
            f"Overall customer rating: {float(avg_rating):.2f}/5.0 "
            f"based on {review_n} reviews."
        )
    else:
        rating_line = "Overall customer rating: no reviews recorded."

    cancel_line = (
        f"Lifetime payment-level issues: {cancelled_ytd} cancelled, "
        f"{refunded_ytd} refunded."
    )

    if rev_share is not None:
        share_line = (
            f"Revenue share in most recent period: {float(rev_share):.1f}%."
        )
    else:
        share_line = (
            "Revenue share in most recent period: not applicable "
            "(staff member was not active in the latest period)."
        )

    parts = [title, intro, status_line]
    if hire_line:
        parts.append(hire_line)
    parts.extend([
        visits_line, revenue_line, commission_line,
        rating_line, cancel_line, share_line,
    ])
    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK 3 — staff_attendance
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_staff_attendance(row: dict[str, Any]) -> str:
    """Render an attendance row as a chunk."""
    name             = row.get("staff_full_name") or "Unknown staff"
    location         = row.get("location_name") or "Unknown location"
    period           = row.get("period_label") or "Unknown period"
    is_active        = bool(row.get("is_active", True))
    hours            = float(row.get("total_hours_worked") or 0.0)
    days_signin      = int(row.get("days_with_signin") or 0)
    days_complete    = int(row.get("days_fully_recorded") or 0)
    days_missing_out = int(row.get("days_missing_signout") or 0)
    avg_per_day      = float(row.get("avg_hours_per_day") or 0.0)

    title_status = " (DEACTIVATED)" if not is_active else ""
    title = f"Staff Attendance — {name}{title_status} — {location} — {period}"

    synonyms = _SYNONYMS_INACTIVE if not is_active else _SYNONYMS_ACTIVE
    intro = (
        f"Staff member: {name} (also referred to as "
        f"{', '.join(synonyms.split(', '))}). Location / branch: {location}."
    )

    if is_active:
        status_line = f"{name} is currently an active staff member."
    else:
        status_line = (
            f"{name} is no longer active — DEACTIVATED. Historical attendance "
            f"data shown for reference."
        )

    hours_line = (
        f"Total hours worked (clocked in): {hours:.1f} hours. "
        f"Average hours per day worked: {avg_per_day:.2f}."
    )
    days_line = (
        f"Days with sign-in recorded: {days_signin}. "
        f"Days fully recorded (both sign-in and sign-out): {days_complete}. "
        f"Days missing sign-out (data quality issue): {days_missing_out}."
    )

    return "\n".join([title, intro, status_line, hours_line, days_line])


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK 4 — staff_rollup (synthesized from staff_monthly)
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_staff_for_period(monthly_rows: list[dict]) -> list[dict]:
    """
    Roll up monthly rows by staff_id (one staff member can appear at multiple
    locations in the same period — sum their numbers across locations).
    """
    by_staff: dict[Any, dict] = {}
    for r in monthly_rows:
        sid = r.get("staff_id")
        if sid is None:
            continue
        agg = by_staff.setdefault(sid, {
            "staff_id":         sid,
            "staff_full_name":  r.get("staff_full_name") or "Unknown",
            "is_active":        bool(r.get("is_active", True)),
            "revenue":          0.0,
            "visits":           0,
            "commission":       0.0,
            "tips":             0.0,
            "rating_sum":       0.0,
            "rating_weight":    0,
            "locations":        set(),
        })
        agg["revenue"]    += float(r.get("revenue") or 0.0)
        agg["visits"]     += int(r.get("completed_visit_count") or 0)
        agg["commission"] += float(r.get("commission_earned") or 0.0)
        agg["tips"]       += float(r.get("tips") or 0.0)
        loc = r.get("location_name")
        if loc:
            agg["locations"].add(loc)
        rating  = r.get("avg_rating")
        reviews = int(r.get("review_count") or 0)
        if rating is not None and reviews > 0:
            agg["rating_sum"]    += float(rating) * reviews
            agg["rating_weight"] += reviews

    for agg in by_staff.values():
        if agg["rating_weight"] > 0:
            agg["avg_rating"] = agg["rating_sum"] / agg["rating_weight"]
        else:
            agg["avg_rating"] = None
        del agg["rating_sum"], agg["rating_weight"]

    return list(by_staff.values())


def _split_into_tiers(
    active_staff: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Split active staff into top 25% / mid 50% / low 25% by revenue.
    Edge cases for n<4: top=1, mid=rest, low=0 (small-org rule).
    """
    if not active_staff:
        return [], [], []

    sorted_staff = sorted(active_staff, key=lambda s: s["revenue"], reverse=True)
    n = len(sorted_staff)

    if n < 4:
        return sorted_staff[:1], sorted_staff[1:], []

    top_n = max(1, math.ceil(n * 0.25))
    low_n = max(1, math.ceil(n * 0.25))
    mid_n = n - top_n - low_n

    top_tier = sorted_staff[:top_n]
    mid_tier = sorted_staff[top_n:top_n + mid_n] if mid_n > 0 else []
    low_tier = sorted_staff[top_n + mid_n:] if low_n > 0 else []

    return top_tier, mid_tier, low_tier


def _format_staff_line(rank: int, agg: dict) -> str:
    """One-line staff summary for the rollup tier listings."""
    name       = agg["staff_full_name"]
    revenue    = agg["revenue"]
    visits     = agg["visits"]
    rating     = agg["avg_rating"]
    commission = agg["commission"]
    rating_str = f"{rating:.1f}★" if rating is not None else "no rating"
    return (
        f"{rank}. {name} — {_money(revenue)}, {visits} visits, "
        f"{rating_str}, {_money(commission)} commission"
    )


def _build_rollup_chunk(period_label: str, monthly_rows: list[dict]) -> str:
    """
    Build the rollup chunk for one period across all monthly rows.

    Structure:
      - Title with period and active/inactive count
      - Header callouts: top performer, highest rated, most visits, org totals
      - Top tier section (top N by revenue)
      - Mid tier section (top N by revenue)
      - Low tier section (BOTTOM N by revenue — the underperformers)
      - Inactive/zero-visit section
      - Synonym block for vocabulary coverage
    """
    aggregated = _aggregate_staff_for_period(monthly_rows)
    active   = [a for a in aggregated if a["is_active"] and a["visits"] > 0]
    inactive = [a for a in aggregated if not a["is_active"] or a["visits"] == 0]

    top_tier, mid_tier, low_tier = _split_into_tiers(active)

    org_revenue = sum(a["revenue"] for a in active)
    n_active    = len(active)
    n_inactive  = len(inactive)
    n_locations = len({loc for a in aggregated for loc in a["locations"]})

    title = (
        f"Staff Performance Rollup — {period_label} — "
        f"{n_active} active, {n_inactive} inactive"
    )

    if active:
        top_by_rev = max(active, key=lambda a: a["revenue"])
        rev_pct    = (top_by_rev["revenue"] / org_revenue * 100) if org_revenue > 0 else 0
        top_perf_line = (
            f"Top performer this period: {top_by_rev['staff_full_name']} "
            f"({_money(top_by_rev['revenue'])} revenue, {rev_pct:.1f}% of org total)."
        )

        rated = [a for a in active if a["avg_rating"] is not None]
        if rated:
            top_rated = max(rated, key=lambda a: a["avg_rating"])
            top_rated_line = (
                f"Highest rated: {top_rated['staff_full_name']} "
                f"({top_rated['avg_rating']:.1f}/5.0)."
            )
        else:
            top_rated_line = "Highest rated: no ratings recorded this period."

        most_visits = max(active, key=lambda a: a["visits"])
        most_visits_line = (
            f"Most visits: {most_visits['staff_full_name']} "
            f"({most_visits['visits']} completed)."
        )

        totals_line = (
            f"Total org revenue this period: {_money(org_revenue)} across "
            f"{n_active} active staff at {n_locations} locations."
        )
    else:
        top_perf_line    = "No active staff with visits this period."
        top_rated_line   = ""
        most_visits_line = ""
        totals_line      = (
            f"No active staff revenue this period. "
            f"{n_inactive} inactive/zero-visit staff on roster."
        )

    sections = []

    if top_tier:
        listed = top_tier[:_ROLLUP_N_PER_TIER]
        more   = max(0, len(top_tier) - len(listed))
        more_str = f" ... and {more} more in this tier." if more > 0 else ""
        section = (
            f"-- Top tier (top 25% by revenue) -- {len(top_tier)} staff --\n"
            + "\n".join(_format_staff_line(i + 1, a) for i, a in enumerate(listed))
            + more_str
        )
        sections.append(section)
    else:
        sections.append("-- Top tier -- no staff in this tier this period --")

    if mid_tier:
        listed = mid_tier[:_ROLLUP_N_PER_TIER]
        more   = max(0, len(mid_tier) - len(listed))
        more_str = f" ... and {more} more in this tier." if more > 0 else ""
        section = (
            f"-- Mid tier (middle 50%) -- {len(mid_tier)} staff --\n"
            + "\n".join(_format_staff_line(i + 1, a) for i, a in enumerate(listed))
            + more_str
        )
        sections.append(section)
    else:
        sections.append("-- Mid tier -- no staff in this tier this period --")

    if low_tier:
        # Reverse to ascending then take first N → lowest performers first
        lowest = list(reversed(low_tier))[:_ROLLUP_N_PER_TIER]
        more   = max(0, len(low_tier) - len(lowest))
        more_str = f" ... and {more} more in this tier." if more > 0 else ""
        section = (
            f"-- Low tier (bottom 25% -- watch list) -- {len(low_tier)} staff --\n"
            + "\n".join(_format_staff_line(i + 1, a) for i, a in enumerate(lowest))
            + more_str
        )
        sections.append(section)
    else:
        sections.append("-- Low tier -- no staff in this tier this period --")

    if inactive:
        inactive_lines = []
        for a in inactive:
            label = " (DEACTIVATED)" if not a["is_active"] else " (zero visits)"
            inactive_lines.append(f"- {a['staff_full_name']}{label}")
        section = (
            f"-- Inactive / zero-visit staff -- {len(inactive)} --\n"
            + "\n".join(inactive_lines)
        )
        sections.append(section)

    synonym_footer = (
        "Vocabulary: top performer, MVP, best worker, highest earner, "
        "ranking, leaderboard, team performance, who is the best, "
        "comparison, bonuses, watch list, underperformer, "
        "deactivated, former employee."
    )

    parts = [title, top_perf_line]
    if top_rated_line:
        parts.append(top_rated_line)
    if most_visits_line:
        parts.append(most_visits_line)
    parts.append(totals_line)
    parts.extend(sections)
    parts.append(synonym_footer)
    return "\n".join(parts)


def _build_rollup_rows(monthly_rows: list[dict]) -> list[dict]:
    """
    Group monthly rows by period_label and build one rollup row per period.
    Each rollup row carries the pre-rendered chunk_text + metadata for embedding.
    """
    by_period: dict[str, list[dict]] = defaultdict(list)
    for r in monthly_rows:
        period = r.get("period_label")
        if period:
            by_period[period].append(r)

    rollup_rows: list[dict] = []
    for period, rows in sorted(by_period.items()):
        chunk = _build_rollup_chunk(period, rows)
        rollup_rows.append({
            "period_label":      period,
            "chunk_text":        chunk,
            "n_staff_in_period": len({r.get("staff_id") for r in rows}),
        })
    return rollup_rows


def _chunk_staff_rollup(row: dict[str, Any]) -> str:
    """
    Pass-through generator for rollup rows. The chunk is pre-rendered by
    _build_rollup_chunk during _build_rollup_rows; this just returns it.
    """
    return row.get("chunk_text") or "(empty rollup)"


# ─────────────────────────────────────────────────────────────────────────────
# Generator dispatch
# ─────────────────────────────────────────────────────────────────────────────

CHUNK_GENERATORS = {
    "staff_monthly":    _chunk_staff_monthly,
    "staff_summary":    _chunk_staff_summary,
    "staff_attendance": _chunk_staff_attendance,
    "staff_rollup":     _chunk_staff_rollup,
}


def _make_doc_id(tenant_id: str, doc_type: str, row: dict[str, Any]) -> str:
    """Build a stable doc_id matching the convention in vector_store.py."""
    if doc_type == "staff_summary":
        suffix = _doc_id_hash(tenant_id, doc_type, str(row.get("staff_id", "")))
    elif doc_type == "staff_rollup":
        # One rollup per (tenant, period) — no staff_id, no location_id
        suffix = _doc_id_hash(tenant_id, doc_type, str(row.get("period_label", "")))
    else:
        suffix = _doc_id_hash(
            tenant_id,
            doc_type,
            str(row.get("staff_id", "")),
            str(row.get("location_id", "")),
            str(row.get("period_label", "")),
        )
    return f"staff:{tenant_id}:{doc_type}:{suffix}"


# ─────────────────────────────────────────────────────────────────────────────
# Main handler — called by DocGenerator._gen_staff()
# Signature MUST match: (org_id, warehouse_rows, embedding_client, vector_store, force)
# ─────────────────────────────────────────────────────────────────────────────

async def generate_staff_docs(
    org_id: int,
    warehouse_rows: list[dict],
    embedding_client: Any,
    vector_store: Any,
    force: bool = False,
) -> dict[str, int]:
    """
    Generate and embed all staff documents for one org.

    Inputs are a FLAT list of warehouse rows. Each row carries a 'doc_type'
    field (staff_monthly | staff_summary | staff_attendance) as produced by
    StaffExtractor.run().

    The 4th doc type — staff_rollup — is synthesized inside this function
    from the staff_monthly subset. No new ETL or warehouse table required.

    Parameters
    ----------
    org_id:
        The tenant / business ID.
    warehouse_rows:
        Documents produced by StaffExtractor.run() and stored in the warehouse.
    embedding_client:
        EmbeddingClient instance — used to embed chunk_text.
    vector_store:
        VectorStore instance — used to upsert embeddings.
    force:
        If True, re-embed even if the doc_id already exists.

    Returns
    -------
    dict with keys: docs_created, docs_skipped, docs_failed
    """
    created = skipped = failed = 0

    # 1. Synthesize rollup rows from the staff_monthly subset.
    #    Pure transformation — no new data fetched.
    monthly_rows = [r for r in warehouse_rows if r.get("doc_type") == "staff_monthly"]
    rollup_rows  = _build_rollup_rows(monthly_rows) if monthly_rows else []
    for rr in rollup_rows:
        rr["doc_type"] = "staff_rollup"  # tag so the dispatch loop picks it up

    # 2. Combined work list: original ETL rows + synthesized rollups
    all_rows = list(warehouse_rows) + rollup_rows

    # 3. Embed and upsert each row
    for row in all_rows:
        doc_type = row.get("doc_type")

        if doc_type not in STAFF_DOC_TYPES:
            logger.debug("staff handler: skipping unknown doc_type=%s", doc_type)
            continue

        doc_id = _make_doc_id(str(org_id), doc_type, row)

        if not force and await vector_store.exists(str(org_id), doc_id):
            skipped += 1
            continue

        chunk_fn = CHUNK_GENERATORS.get(doc_type)
        if chunk_fn is None:
            logger.warning("staff handler: no chunk generator for doc_type=%s", doc_type)
            failed += 1
            continue

        try:
            chunk_text   = chunk_fn(row)
            embedding    = await embedding_client.embed(chunk_text)
            period_start = _parse_period_start(row.get("period_label"))

            metadata = {
                "org_id":            org_id,
                "doc_type":          doc_type,
                "domain":            DOMAIN,
                "staff_id":          row.get("staff_id"),
                "staff_name":        row.get("staff_full_name"),
                "period":            row.get("period_label"),
                "location_id":       row.get("location_id"),
                "location_name":     row.get("location_name"),
                "is_active":         row.get("is_active"),
                "n_staff_in_period": row.get("n_staff_in_period"),
            }

            await vector_store.upsert(
                doc_id=doc_id,
                tenant_id=str(org_id),
                doc_domain=DOMAIN,
                doc_type=doc_type,
                chunk_text=chunk_text,
                embedding=embedding,
                period_start=period_start,
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
        "staff handler done org=%d created=%d skipped=%d failed=%d (incl. %d rollups)",
        org_id, created, skipped, failed, len(rollup_rows),
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}