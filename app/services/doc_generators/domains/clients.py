"""
app/services/doc_generators/domains/clients.py
===============================================
Clients domain chunk generator.

Transforms warehouse rows (from wh_client_retention, wh_client_cohort_monthly,
wh_client_per_location_monthly) into ~15-20 embedding documents per business.

Doc catalog (matches Step 2 Internal Supplement §1):
   1. counts_rollup           — Q1, Q2, Q16
   2. mom_narrative           — Q4, Q10
   3. churn_summary           — Q5, Q12
   4. reactivation_summary    — Q6
   5. top_ltv                 — Q7
   6. top_frequency           — Q8
   7. top_points              — Q9
   8. at_risk                 — Q11, Q14
   9. ltv_tiers               — Q19
  10. reachability            — Q18
  11. age_distribution        — Q21
  12. member_overlap          — Q22
  13. unique_visitors         — Q23
  14. all_time_honesty        — Q17
  15. retention_advice        — Q15
  16..N. per_location         — Q20 (is_rollup=False for filter isolation)

Total: 15 rollup docs + 1 per location ≈ 15-20 docs per business.
Scales with location count, NOT client count.

PII policy:
  NO CLIENT NAMES IN ANY CHUNK. All per-client references use "Client #N"
  (where N = ltv_rank, frequency_rank, etc.). Defense in depth — even if
  names leaked into the warehouse, the chunk generator would strip them.

Vocabulary engineering:
  Each chunk embeds synonym sets in its text so vector retrieval wins on
  the right doc for the right question. See per-function comments.
"""

from __future__ import annotations

import logging
from typing import Iterable

logger = logging.getLogger(__name__)

DOMAIN = "clients"

# Doc types — used as stable doc_id component
DOC_TYPE_COUNTS              = "counts_rollup"
DOC_TYPE_MOM                 = "mom_narrative"
DOC_TYPE_CHURN               = "churn_summary"
DOC_TYPE_REACTIVATION        = "reactivation_summary"
DOC_TYPE_TOP_LTV             = "top_ltv"
DOC_TYPE_TOP_FREQUENCY       = "top_frequency"
DOC_TYPE_TOP_POINTS          = "top_points"
DOC_TYPE_AT_RISK             = "at_risk"
DOC_TYPE_LTV_TIERS           = "ltv_tiers"
DOC_TYPE_REACHABILITY        = "reachability"
DOC_TYPE_AGE_DIST            = "age_distribution"
DOC_TYPE_MEMBER_OVERLAP      = "member_overlap"
DOC_TYPE_UNIQUE_VISITORS     = "unique_visitors"
DOC_TYPE_ALL_TIME            = "all_time_honesty"
DOC_TYPE_RETENTION_ADVICE    = "retention_advice"
DOC_TYPE_PER_LOCATION        = "per_location"


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point — called by doc_generators/__init__.py (_gen_clients)
# ─────────────────────────────────────────────────────────────────────────────

async def generate_client_docs(
    org_id: int,
    warehouse_rows: dict,
    embedding_client,
    vector_store,
    force: bool = False,
) -> dict:
    """
    Build chunks from warehouse rows, embed, and upsert to pgvector.

    Parameters
    ----------
    org_id:          Tenant / business ID.
    warehouse_rows:  {"retention_snapshot": [...], "cohort_monthly": [...],
                      "per_location": [...]}  as produced by ClientsExtractor.run().
    embedding_client: EmbeddingClient — embeds chunk_text.
    vector_store:    VectorStore — upserts embeddings.
    force:           If True, re-embed even if doc_id already exists.

    Returns
    -------
    dict with: docs_created, docs_skipped, docs_failed
    """
    snapshot = warehouse_rows.get("retention_snapshot", [])
    cohort = warehouse_rows.get("cohort_monthly", [])
    per_loc = warehouse_rows.get("per_location", [])

    chunks: list[tuple[str, str, str, bool, dict]] = []
    # Tuple = (doc_id, doc_type, chunk_text, is_rollup, metadata)

    # Determine the "current period" — the most recent period with data
    if cohort:
        curr_period = max(cohort, key=lambda r: str(r.get("period", "")))
        period_label = str(curr_period.get("period", ""))[:7]  # YYYY-MM
    else:
        curr_period = None
        period_label = "recent"

    # Build chunks in order
    if curr_period:
        chunks.extend(_build_counts_rollup(org_id, curr_period, period_label))
        chunks.extend(_build_mom_narrative(org_id, cohort, period_label))
        chunks.extend(_build_churn_summary(org_id, curr_period, period_label))
        chunks.extend(_build_reactivation_summary(org_id, curr_period, period_label))
        chunks.extend(_build_ltv_tiers(org_id, curr_period, period_label))
        chunks.extend(_build_reachability(org_id, curr_period, period_label))
        chunks.extend(_build_member_overlap(org_id, curr_period, period_label))
        chunks.extend(_build_unique_visitors(org_id, curr_period, period_label))

    if snapshot:
        chunks.extend(_build_top_ltv(org_id, snapshot, period_label))
        chunks.extend(_build_top_frequency(org_id, snapshot, period_label))
        chunks.extend(_build_top_points(org_id, snapshot, period_label))
        chunks.extend(_build_at_risk(org_id, snapshot, period_label))
        chunks.extend(_build_age_distribution(org_id, snapshot, period_label))
        chunks.extend(_build_all_time(org_id, snapshot, cohort, period_label))
        chunks.extend(_build_retention_advice(org_id, snapshot, curr_period, period_label))

    if per_loc:
        chunks.extend(_build_per_location(org_id, per_loc, period_label))

    # ── Embed + upsert ────────────────────────────────────────────────────────
    created = skipped = failed = 0
    for doc_id, doc_type, chunk_text, is_rollup, metadata in chunks:
        if not force and await vector_store.exists(doc_id):
            skipped += 1
            continue

        try:
            embedding = await embedding_client.embed(chunk_text)
            await vector_store.upsert(
                doc_id=doc_id,
                tenant_id=str(org_id),
                doc_domain=DOMAIN,
                doc_type=doc_type,
                chunk_text=chunk_text,
                embedding=embedding,
                metadata={**metadata, "is_rollup": is_rollup},
            )
            created += 1
        except Exception as exc:
            failed += 1
            logger.error(
                "clients handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "clients handler done org=%d created=%d skipped=%d failed=%d "
        "(total_chunks=%d)",
        org_id, created, skipped, failed, len(chunks),
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK BUILDERS — one function per doc type
# Each returns a list of (doc_id, doc_type, chunk_text, is_rollup, metadata)
# ─────────────────────────────────────────────────────────────────────────────

def _build_counts_rollup(org_id, row, period_label):
    """Doc 1 — Q1, Q2, Q16.
    Vocabulary: client, customer, new, returning, active, on file, acquired."""
    total     = row.get("clients_total", 0)
    new_c     = row.get("new_clients", 0)
    ret_c     = row.get("returning_clients", 0)
    active    = row.get("active_clients_in_period", 0)
    split     = row.get("new_vs_returning_split")
    split_txt = f"{split:.1f}% new, {100-split:.1f}% returning" if split is not None else "no split available"

    text = (
        f"Client summary for {period_label}. "
        f"Total clients on file: {total}. "
        f"New clients acquired this period: {new_c}. "
        f"Returning clients (visited this period and had prior history): {ret_c}. "
        f"New versus returning split: {split_txt}. "
        f"Active client base (had at least one visit in period): {active}."
    )
    doc_id = f"clients:{org_id}:counts:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_COUNTS, text, True, meta)]


def _build_mom_narrative(org_id, cohort_rows, period_label):
    """Doc 2 — Q4, Q10. Needs at least current + prior period."""
    if len(cohort_rows) < 2:
        return []
    by_period = sorted(cohort_rows, key=lambda r: str(r.get("period", "")), reverse=True)
    curr, prev = by_period[0], by_period[1]

    curr_new = curr.get("new_clients", 0)
    prev_new = prev.get("new_clients", 0)
    mom_pct  = curr.get("new_clients_mom_pct")
    prev_label = str(prev.get("period", ""))[:7]

    if mom_pct is None:
        direction, delta, magnitude = "held steady", "", "flat"
    elif mom_pct < 0:
        direction = "decreased"
        delta     = f"({mom_pct:+.1f}%)"
        magnitude = "sharp" if abs(mom_pct) > 25 else "significant" if abs(mom_pct) > 10 else "modest"
    else:
        direction = "increased"
        delta     = f"({mom_pct:+.1f}%)"
        magnitude = "sharp" if mom_pct > 25 else "significant" if mom_pct > 10 else "modest"

    at_risk_mom = curr.get("at_risk_mom_pct")
    at_risk_dir = "rose" if (at_risk_mom or 0) > 0 else "fell"
    at_risk_txt = (
        f"At-risk count {at_risk_dir} by {abs(at_risk_mom):.1f}% from the prior month. "
        if at_risk_mom is not None else ""
    )

    text = (
        f"Client acquisition trend: {period_label} versus {prev_label}. "
        f"New clients {direction} from {prev_new} to {curr_new} {delta}. "
        f"This is a {magnitude} change compared to the prior month. "
        f"{at_risk_txt}"
        f"Unique visitors moved from {prev.get('unique_visitors_in_period', 0)} to "
        f"{curr.get('unique_visitors_in_period', 0)}."
    )
    doc_id = f"clients:{org_id}:mom:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_MOM, text, True, meta)]


def _build_churn_summary(org_id, row, period_label):
    """Doc 3 — Q5, Q12.
    Vocabulary: churn, retention, lost, at risk, dormant, came back."""
    at_risk  = row.get("at_risk_clients", 0)
    retention = row.get("retention_rate_pct")
    churn    = row.get("churn_rate_pct")
    threshold = 60   # default — matches Step 1 decision D4

    retention_txt = (
        f"Cohort retention rate: {retention:.1f}% of clients who visited in the prior "
        f"period came back and visited again in {period_label}."
        if retention is not None else
        f"Cohort retention rate could not be computed for {period_label} — prior period had no actives."
    )
    churn_txt = f"Churn rate: {churn:.1f}% of clients are at risk." if churn is not None else ""

    text = (
        f"Retention and churn for {period_label}. "
        f"At-risk clients (no visit in over {threshold} days): {at_risk}. "
        f"{retention_txt} {churn_txt} "
        f"The churn threshold is configurable per business; the default is {threshold} days."
    )
    doc_id = f"clients:{org_id}:churn:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_CHURN, text, True, meta)]


def _build_reactivation_summary(org_id, row, period_label):
    """Doc 4 — Q6.
    Vocabulary: reactivate, win back, returned, came back, dormant."""
    reactivated = row.get("reactivated_clients", 0)
    text = (
        f"Reactivation for {period_label}. "
        f"Clients who returned after a 90-day absence: {reactivated}. "
        f"These are previously-dormant clients who came back this period — "
        f"a positive signal for win-back and reactivation efforts."
    )
    doc_id = f"clients:{org_id}:reactivation:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_REACTIVATION, text, True, meta)]


def _build_top_ltv(org_id, snapshot, period_label):
    """Doc 5 — Q7.
    Vocabulary: LTV, lifetime value, lifetime spend, biggest spenders,
    regulars, best customers, top spenders."""
    top10 = sorted(
        [c for c in snapshot if c.get("ltv_rank")],
        key=lambda c: c["ltv_rank"],
    )[:10]
    if not top10:
        return []

    lines = []
    total_top10 = 0.0
    for c in top10:
        rank = c["ltv_rank"]
        ltv = c.get("lifetime_revenue", 0)
        visits = c.get("total_visits_ever", 0)
        loc = c.get("home_location_name") or "unknown location"
        lines.append(
            f"{rank}. Client #{c['client_id']} — lifetime spend ${ltv:,.2f}, "
            f"{visits} visits, home location: {loc}."
        )
        total_top10 += ltv

    text = (
        "Top clients by lifetime spend — also known as LTV, lifetime value, "
        "lifetime revenue, biggest spenders, regulars, best customers, or "
        "top spenders. " +
        " ".join(lines) +
        f" Combined, these top {len(top10)} clients represent ${total_top10:,.2f} "
        "in lifetime revenue."
    )
    doc_id = f"clients:{org_id}:top_ltv"
    meta = {"business_id": org_id}
    return [(doc_id, DOC_TYPE_TOP_LTV, text, True, meta)]


def _build_top_frequency(org_id, snapshot, period_label):
    """Doc 6 — Q8.
    Vocabulary: frequent, loyal, regular, most visits, top visitors."""
    top10 = sorted(
        [c for c in snapshot if c.get("frequency_rank")],
        key=lambda c: c["frequency_rank"],
    )[:10]
    if not top10:
        return []

    lines = [
        f"{c['frequency_rank']}. Client #{c['client_id']} — "
        f"{c.get('visits_in_period', 0)} visits this period."
        for c in top10
    ]
    text = (
        f"Most frequent clients in {period_label} — most visits, loyal clients, "
        f"top visitors, regulars. " +
        " ".join(lines)
    )
    doc_id = f"clients:{org_id}:top_frequency:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_TOP_FREQUENCY, text, True, meta)]


def _build_top_points(org_id, snapshot, period_label):
    """Doc 7 — Q9.
    Vocabulary: loyalty points, points, rewards, points holders."""
    top10 = sorted(
        [c for c in snapshot if c.get("points_rank")],
        key=lambda c: c["points_rank"],
    )[:10]
    if not top10:
        return []

    lines = [
        f"{c['points_rank']}. Client #{c['client_id']} — {c.get('points', 0):.0f} points."
        for c in top10
    ]
    text = (
        "Clients with the most loyalty points — rewards, points balance, "
        "points holders. " +
        " ".join(lines) +
        " Points are earned through the business's loyalty program and can "
        "be redeemed per the program terms."
    )
    doc_id = f"clients:{org_id}:top_points"
    meta = {"business_id": org_id}
    return [(doc_id, DOC_TYPE_TOP_POINTS, text, True, meta)]


def _build_at_risk(org_id, snapshot, period_label):
    """Doc 8 — Q11, Q14.
    Vocabulary: at risk, risk of churning, haven't seen, win back, reach out."""
    at_risk = [c for c in snapshot if c.get("at_risk_flag")]
    if not at_risk:
        return []

    # Top 20 at-risk by LTV (highest priority for reactivation)
    at_risk_sorted = sorted(
        at_risk,
        key=lambda c: -(c.get("lifetime_revenue") or 0),
    )[:20]

    reachable_count = sum(
        1 for c in at_risk
        if c.get("is_reachable_email") or c.get("is_reachable_sms")
    )

    lines = []
    for c in at_risk_sorted:
        reach_flags = []
        if c.get("is_reachable_email"): reach_flags.append("email")
        if c.get("is_reachable_sms"): reach_flags.append("SMS")
        reach_txt = "/".join(reach_flags) if reach_flags else "unreachable"
        lines.append(
            f"Client #{c['client_id']} — {c.get('days_since_last_visit', '?')} "
            f"days since last visit, lifetime spend ${c.get('lifetime_revenue', 0):,.2f}, "
            f"reachable by {reach_txt}."
        )

    text = (
        f"At-risk clients — no visit in over 60 days. "
        f"Total at-risk: {len(at_risk)}. Of these, {reachable_count} are still "
        f"reachable by email or SMS. "
        f"Top {len(at_risk_sorted)} at-risk by lifetime value (highest priority "
        f"for reactivation outreach): " +
        " ".join(lines) +
        " Recommended action: reach out to the reachable high-value at-risk "
        "clients first — a targeted SMS or email offer may win them back."
    )
    doc_id = f"clients:{org_id}:at_risk"
    meta = {"business_id": org_id}
    return [(doc_id, DOC_TYPE_AT_RISK, text, True, meta)]


def _build_ltv_tiers(org_id, row, period_label):
    """Doc 9 — Q19."""
    top10pct = row.get("top10pct_revenue_share")
    total_rev = row.get("total_revenue_in_period", 0)
    n = row.get("active_clients_in_period", 0)
    if top10pct is None:
        return []

    text = (
        f"Revenue concentration for {period_label}. "
        f"Top 10 percent of clients generated {top10pct:.1f}% of total period revenue. "
        f"Total period revenue: ${total_rev:,.2f}. "
        f"Total active clients in analysis: {n}. "
        f"This is the Pareto distribution of the client base — if the top 10% "
        f"generates a disproportionate share, the business is VIP-driven; "
        f"if the share is close to 10%, it is volume-driven."
    )
    doc_id = f"clients:{org_id}:ltv_tiers:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_LTV_TIERS, text, True, meta)]


def _build_reachability(org_id, row, period_label):
    """Doc 10 — Q18."""
    reach_email = row.get("reachable_email", 0)
    reach_sms = row.get("reachable_sms", 0)
    active = row.get("active_clients_in_period", 0)
    email_pct = (reach_email / active * 100) if active else 0
    sms_pct = (reach_sms / active * 100) if active else 0

    text = (
        f"Client contactability summary as of {period_label}. "
        f"Active clients who can still be emailed: {reach_email} of {active} "
        f"({email_pct:.1f}%). "
        f"Active clients who can still receive SMS: {reach_sms} of {active} "
        f"({sms_pct:.1f}%). "
        f"Reasons a client cannot be contacted: unsubscribed, no email or phone "
        f"on file, or soft-deleted from the tenant."
    )
    doc_id = f"clients:{org_id}:reachability"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_REACHABILITY, text, True, meta)]


def _build_age_distribution(org_id, snapshot, period_label):
    """Doc 11 — Q21. Filtered to new-in-period clients."""
    new_clients = [c for c in snapshot if c.get("is_new_in_period")]
    if not new_clients:
        return []

    from collections import Counter
    buckets = Counter(c.get("age_bracket") or "unknown" for c in new_clients)
    total = len(new_clients)

    def _line(label, key):
        n = buckets.get(key, 0)
        pct = (n / total * 100) if total else 0
        return f"{label}: {n} ({pct:.1f}%)"

    lines = [
        _line("Under 25", "under_25"),
        _line("25 to 40", "25_to_40"),
        _line("40 to 55", "40_to_55"),
        _line("55 and over", "55_plus"),
        _line("Unknown (no date of birth on file)", "unknown"),
    ]

    text = (
        f"Age breakdown of new clients acquired in {period_label}. " +
        ". ".join(lines) + "."
    )
    doc_id = f"clients:{org_id}:age_dist:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_AGE_DIST, text, True, meta)]


def _build_member_overlap(org_id, row, period_label):
    """Doc 12 — Q22."""
    members = row.get("active_members", 0)
    active = row.get("active_clients_in_period", 0)
    overlap = row.get("member_overlap_pct")
    non_members = active - members

    if overlap is None:
        return []

    text = (
        f"Membership overlap with active client base for {period_label}. "
        f"Active clients who are also members: {members} of {active} "
        f"({overlap:.1f}%). "
        f"Non-member active clients: {non_members}. "
        f"For deeper membership analytics — renewal rates, monthly recurring "
        f"revenue, lapsed members — see the Memberships domain."
    )
    doc_id = f"clients:{org_id}:member_overlap:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_MEMBER_OVERLAP, text, True, meta)]


def _build_unique_visitors(org_id, row, period_label):
    """Doc 13 — Q23 (dedup across walk-ins + bookings).
    Vocabulary: unique, walk-in, booking, distinct, how many people."""
    unique = row.get("unique_visitors_in_period", 0)
    text = (
        f"Unique clients who visited in {period_label}: {unique}. "
        f"This count is deduplicated — each client is counted once regardless "
        f"of how many visits they made or how they arrived (booked appointment, "
        f"app sign-in, or walk-in). Bookings and walk-ins are combined into a "
        f"single unified visit record, so there is no double-counting across "
        f"channels."
    )
    doc_id = f"clients:{org_id}:unique_visitors:{period_label}"
    meta = {"period": period_label, "business_id": org_id}
    return [(doc_id, DOC_TYPE_UNIQUE_VISITORS, text, True, meta)]


def _build_all_time(org_id, snapshot, cohort_rows, period_label):
    """Doc 14 — Q17 (all-time honest answer)."""
    total_unique = len(set(c["client_id"] for c in snapshot))
    periods = sorted({str(r.get("period", ""))[:7] for r in cohort_rows if r.get("period")})
    window_start = periods[0] if periods else period_label
    window_end = periods[-1] if periods else period_label

    text = (
        f"Based on the available data from {window_start} to {window_end}, "
        f"this business has {total_unique} unique clients on file. "
        f"Data from before {window_start} is not in the analysis window — "
        f"the full historical client count may be higher than this figure. "
        f"When asked about total customers or clients ever, this figure "
        f"reflects only what the analytics window contains."
    )
    doc_id = f"clients:{org_id}:alltime"
    meta = {"business_id": org_id, "window_start": window_start, "window_end": window_end}
    return [(doc_id, DOC_TYPE_ALL_TIME, text, True, meta)]


def _build_retention_advice(org_id, snapshot, cohort_row, period_label):
    """Doc 15 — Q15 (retention advice)."""
    at_risk = [c for c in snapshot if c.get("at_risk_flag")]
    reachable_at_risk = [
        c for c in at_risk
        if c.get("is_reachable_email") or c.get("is_reachable_sms")
    ]
    non_member_active = [
        c for c in snapshot
        if c.get("visits_in_period", 0) >= 1 and not c.get("is_member")
    ]

    tips = []
    if reachable_at_risk:
        tips.append(
            f"{len(reachable_at_risk)} at-risk clients are still reachable by "
            f"email or SMS — these are the highest-ROI targets for reactivation "
            f"outreach."
        )
    if non_member_active:
        tips.append(
            f"{len(non_member_active)} active non-members could be targeted "
            f"for membership conversion."
        )
    if cohort_row and cohort_row.get("retention_rate_pct") is not None:
        tips.append(
            f"Current cohort retention rate is "
            f"{cohort_row['retention_rate_pct']:.1f}% — industry-healthy is "
            f"above 70%."
        )

    text = (
        "Retention improvement opportunities based on current data: " +
        " ".join(f"- {t}" for t in tips) if tips else
        "No clear retention improvement opportunities detected in the current period."
    )
    doc_id = f"clients:{org_id}:retention_advice"
    meta = {"business_id": org_id, "period": period_label}
    return [(doc_id, DOC_TYPE_RETENTION_ADVICE, text, True, meta)]


def _build_per_location(org_id, per_loc_rows, period_label):
    """Docs 16..N — Q20. Per-location chunks with is_rollup=False.
    Vocabulary: location, branch, site, store, plus the literal location name."""
    out = []
    total_locations = len(per_loc_rows)
    for r in per_loc_rows:
        loc_id = r["location_id"]
        loc_name = r.get("location_name", f"Location {loc_id}")
        new_here = r.get("new_clients_here", 0)
        homed_here = r.get("clients_homed_here", 0)
        active_here = r.get("active_clients_here", 0)
        rank_new = r.get("rank_by_new_clients", "?")
        revenue_here = r.get("revenue_here", 0)

        text = (
            f"Client activity at the {loc_name} location — also called a "
            f"branch, site, or store — for {period_label}. "
            f"New clients acquired at {loc_name} this period: {new_here}. "
            f"Total clients who consider {loc_name} their home location: {homed_here}. "
            f"Active clients visiting {loc_name}: {active_here}. "
            f"Revenue generated from clients at {loc_name}: ${revenue_here:,.2f}. "
            f"{loc_name} ranks #{rank_new} of {total_locations} locations for "
            f"new client acquisition this period."
        )
        doc_id = f"clients:{org_id}:per_location:{loc_id}:{period_label}"
        meta = {
            "period": period_label,
            "business_id": org_id,
            "location_id": loc_id,
            "location_name": loc_name,
        }
        # IMPORTANT: is_rollup=False — enables vector_store.exclude_rollup filter
        # for branch-comparison questions (Q20)
        out.append((doc_id, DOC_TYPE_PER_LOCATION, text, False, meta))
    return out