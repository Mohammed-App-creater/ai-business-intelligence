"""
app/services/doc_generators/domains/memberships.py
====================================================
Memberships Domain — Document Generator (Step 4)

Reads warehouse rows produced by MembershipsExtractor and:
  1. Builds natural-language chunk text per row (unit + monthly summary)
  2. Embeds via embedding_client
  3. Upserts into the vector store with content-hash skip-detection
  4. Returns counts in the standard {"docs_created", "docs_skipped", "docs_failed"} shape

Pattern matches generate_appointments_docs / generate_client_docs /
generate_marketing_docs — keyword args, async, store directly to vector store.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Optional

from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.vector_store import VectorStore

logger = logging.getLogger(__name__)

DOMAIN = "memberships"


# ─────────────────────────────────────────────────────────────────────────────
#  Display helpers
# ─────────────────────────────────────────────────────────────────────────────

# NOTE: LOCATION_NAMES is hardcoded TEMPORARILY for the v1 sprint.
#
# Why hardcoded?
#   During Step 4 wire-up, the doc generator needed location names to render
#   natural-language chunk text ("Membership summary for the Downtown
#   location..."). The proper implementation is a tenant-scoped lookup that
#   queries tbl_organization_locations (or its warehouse mirror) at doc-build
#   time. That requires:
#     - Plumbing a WarehouseClient or pool into this module (currently only
#       embedding_client + vector_store are passed in)
#     - Adding async DB calls inside doc-text builders
#     - Caching to avoid N round-trips per ETL run
#   That work was deferred so we could ship v1.
#
# Effect for real tenants:
#   The IDs below match the test fixture (biz=99 only). Real-customer tenants
#   (biz=40, biz=42, etc.) have different location IDs, so their docs will
#   render with the fallback "Location {id}" naming via _loc_name(). Answers
#   stay numerically correct — only the friendly name is missing.
#
# Replacement plan:
#   When the real backend API integration sprint lands, replace this dict
#   with a tenant-scoped resolver. Search for "LOCATION_NAMES" and "_loc_name"
#   to find every use site.
#
# See Memberships_Sprint_Notes.md §5 (gap C5) for the full record.
LOCATION_NAMES: dict[int, str] = {
    101: "Downtown",
    102: "Westside",
    103: "Northpark",
}

MONTH_LABELS = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December",
}


def _loc_name(loc_id: int) -> str:
    return LOCATION_NAMES.get(loc_id, f"Location {loc_id}")


def _money(v: Any) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _fmt_date(s: Optional[str]) -> Optional[str]:
    """ISO datetime string → 'April 18, 2026'."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return f"{MONTH_LABELS[dt.month]} {dt.day}, {dt.year}"
    except Exception:
        return s


def _month_label(month_start_iso: str) -> str:
    """'2026-02-01' → 'February 2026'."""
    y, m, _ = month_start_iso.split("-")
    return f"{MONTH_LABELS[int(m)]} {y}"


def _ym_suffix(month_start_iso: str) -> str:
    """'2026-02-01' → '2026_02'."""
    y, m, _ = month_start_iso.split("-")
    return f"{y}_{m}"


# ─────────────────────────────────────────────────────────────────────────────
#  Chunk text builders
# ─────────────────────────────────────────────────────────────────────────────

def _unit_chunk_text(row: dict) -> str:
    """Natural-language summary for one membership row."""
    name     = row.get("customer_name") or f"Customer {row['customer_id']}"
    loc      = _loc_name(row["location_id"])
    service  = row.get("service_name") or f"Service {row['service_id']}"
    bucket   = row["interval_bucket"]
    amount   = _money(row["amount"])
    discount = float(row.get("discount") or 0)
    tenure   = row["tenure_days"]
    created  = _fmt_date(row["created_at"])
    billed   = _money(row["total_billed"])
    visits   = int(row.get("visit_count_in_window") or 0)
    approved = int(row.get("approved_charge_count") or 0)
    failed   = int(row.get("failed_charge_count") or 0)

    parts: list[str] = []

    if row["is_active"] == 1:
        parts.append(
            f"Active membership: {name} at the {loc} location holds a "
            f"{service} membership ({bucket} billing, {amount} per cycle"
            + (f" with {_money(discount)} discount" if discount > 0 else "")
            + f"). Tenure: {tenure} days since {created}."
        )
    else:
        canceled = _fmt_date(row.get("canceled_at"))
        parts.append(
            f"Canceled membership: {name} at the {loc} location held a "
            f"{service} membership for {tenure} days from {created} to {canceled}."
        )

    parts.append(
        f"Monthly equivalent revenue: {_money(row['monthly_equivalent_revenue'])}. "
        f"Estimated lifetime value: {_money(row['estimated_ltv'])}. "
        f"Total billed across {approved} successful charge"
        f"{'s' if approved != 1 else ''}: {billed}."
    )

    if failed > 0:
        parts.append(
            f"Has {failed} failed payment attempt{'s' if failed != 1 else ''}."
        )

    if visits > 0:
        last_visit = _fmt_date(row.get("last_visit_at"))
        parts.append(
            f"Customer used the membership {visits} time"
            f"{'s' if visits != 1 else ''}, last visit {last_visit}."
        )
    elif row["is_active"] == 1:
        parts.append(
            "Customer has not yet used the membership — possible churn risk."
        )

    if row.get("is_due_in_7_days") == 1:
        next_d = _fmt_date(row.get("next_execution_date"))
        parts.append(f"Next payment due {next_d} (within 7 days).")

    if row.get("is_reactivation") == 1:
        parts.append(
            "This is a reactivation — the customer previously canceled and "
            "resigned within 90 days for the same service."
        )

    return " ".join(parts)


def _monthly_chunk_text(row: dict) -> str:
    """Natural-language summary for one location-month row."""
    loc      = _loc_name(row["location_id"])
    month    = _month_label(row["month_start"])
    active   = int(row.get("active_at_month_end") or 0)
    mrr      = _money(row.get("mrr"))
    signups  = int(row.get("new_signups") or 0)
    react    = int(row.get("reactivations") or 0)
    cancels  = int(row.get("cancellations") or 0)
    billed   = _money(row.get("gross_billed"))
    approved = int(row.get("approved_charges") or 0)
    failed   = int(row.get("failed_charges") or 0)
    mom      = row.get("mrr_mom_pct")
    churn    = row.get("churn_rate_pct")
    avg_disc = row.get("avg_discount")

    parts: list[str] = []
    parts.append(
        f"Membership summary for the {loc} location in {month}: "
        f"{active} active member{'s' if active != 1 else ''} generating {mrr} MRR. "
        f"Activity: {signups} new signup{'s' if signups != 1 else ''}, "
        f"{react} reactivation{'s' if react != 1 else ''}, "
        f"{cancels} cancellation{'s' if cancels != 1 else ''}."
    )

    if mom is not None:
        direction = "up" if mom > 0 else ("down" if mom < 0 else "flat")
        parts.append(f"MRR change vs prior month: {mom}% ({direction}).")

    if approved > 0 or failed > 0:
        if failed > 0:
            parts.append(
                f"Billed {billed} across {approved} successful charge"
                f"{'s' if approved != 1 else ''} and {failed} failed payment"
                f"{'s' if failed != 1 else ''}."
            )
        else:
            parts.append(
                f"Billed {billed} across {approved} successful charge"
                f"{'s' if approved != 1 else ''} with no failures."
            )

    if churn is not None and churn > 0:
        parts.append(f"Churn rate: {churn}% of prior month's active members.")

    if avg_disc is not None and float(avg_disc) > 0:
        parts.append(f"Average discount among active members: {_money(avg_disc)}.")

    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
#  Rollup builders — synthesis docs that aggregate across all unit rows
#  These exist because cosine retrieval can't aggregate across N per-row
#  docs at query time. Each rollup is one pre-computed answer to a class
#  of business questions.
# ─────────────────────────────────────────────────────────────────────────────

def _build_service_breakdown_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Powers Q8 (which service has the most active memberships)
    and Q11 (% of revenue per service).
    Always emits when there is at least one membership (active or canceled).
    """
    if not units:
        return None

    active = [r for r in units if r.get("is_active") == 1]
    if not active:
        return None

    # Aggregate per service
    by_svc: dict[int, dict] = {}
    for r in active:
        sid = r["service_id"]
        if sid not in by_svc:
            by_svc[sid] = {
                "service_id":   sid,
                "service_name": r.get("service_name") or f"Service {sid}",
                "count":        0,
                "mrr":          0.0,
            }
        by_svc[sid]["count"] += 1
        by_svc[sid]["mrr"]   += float(r.get("monthly_equivalent_revenue") or 0)

    rows = sorted(by_svc.values(), key=lambda x: -x["count"])
    total_count = sum(x["count"] for x in rows)
    total_mrr   = sum(x["mrr"]   for x in rows)

    parts = [
        f"Membership service breakdown across {total_count} active "
        f"membership{'s' if total_count != 1 else ''} totaling {_money(total_mrr)} MRR:"
    ]
    for x in rows:
        pct = (x["mrr"] / total_mrr * 100) if total_mrr else 0
        parts.append(
            f"• {x['service_name']} — {x['count']} active member"
            f"{'s' if x['count'] != 1 else ''}, {_money(x['mrr'])} MRR ({pct:.1f}% of total)"
        )

    top = rows[0]
    if len(rows) > 1 and top["count"] > rows[1]["count"]:
        parts.append(
            f"{top['service_name']} is the dominant membership service by "
            "both count and revenue."
        )

    chunk = "\n".join(parts)
    return {
        "doc_id":     f"{org_id}_memberships_service_breakdown_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_service_breakdown",
        "chunk_text": chunk,
        "metadata": {
            "service_count":      len(rows),
            "active_total":       total_count,
            "mrr_total":          round(total_mrr, 2),
            "top_service":        top["service_name"],
            "top_service_count":  top["count"],
            "as_of_date":         as_of.isoformat(),
        },
    }


def _build_tenure_ranking_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Powers Q10 (longest-tenured members).
    Top 20 active members by tenure_days. Lists fewer when count is < 20.
    """
    active = [r for r in units if r.get("is_active") == 1]
    if not active:
        return None

    ranked = sorted(active, key=lambda r: -(r.get("tenure_days") or 0))[:20]
    n = len(ranked)

    parts = [
        f"Top {n} longest-tenured active member"
        f"{'s' if n != 1 else ''} as of {_fmt_date(as_of.isoformat())}:"
    ]
    for i, r in enumerate(ranked, start=1):
        name    = r.get("customer_name") or f"Customer {r['customer_id']}"
        loc     = _loc_name(r["location_id"])
        service = r.get("service_name") or f"Service {r['service_id']}"
        tenure  = r.get("tenure_days") or 0
        created = _fmt_date(r.get("created_at"))
        disc    = float(r.get("discount") or 0)
        disc_part = f" with {_money(disc)} discount" if disc > 0 else ""
        parts.append(
            f"{i}. {name} ({loc}, {service}{disc_part}) — "
            f"{tenure} day{'s' if tenure != 1 else ''} since {created}"
        )

    chunk = "\n".join(parts)
    return {
        "doc_id":     f"{org_id}_memberships_tenure_ranking_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_tenure_ranking",
        "chunk_text": chunk,
        "metadata": {
            "ranked_count":     n,
            "longest_tenure":   ranked[0].get("tenure_days"),
            "longest_member":   ranked[0].get("customer_name"),
            "as_of_date":       as_of.isoformat(),
        },
    }


def _build_discount_summary_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Powers Q17 (average discount given on memberships).
    Always emits when there are active memberships, even if zero are discounted —
    "no memberships have a discount applied" is a useful answer.
    """
    active = [r for r in units if r.get("is_active") == 1]
    if not active:
        return None

    total_active = len(active)
    discs = [float(r["discount"]) for r in active if float(r.get("discount") or 0) > 0]
    n_disc = len(discs)

    parts = [f"Membership discount summary as of {_fmt_date(as_of.isoformat())}."]

    if n_disc == 0:
        parts.append(
            f"None of the {total_active} active membership"
            f"{'s' if total_active != 1 else ''} has a per-cycle discount applied — "
            "all are billed at full price."
        )
        avg_disc = 0.0
        min_disc = 0.0
        max_disc = 0.0
    else:
        avg_disc = sum(discs) / n_disc
        min_disc = min(discs)
        max_disc = max(discs)
        pct_with_disc = n_disc / total_active * 100
        parts.append(
            f"Of {total_active} active memberships, {n_disc} ({pct_with_disc:.0f}%) "
            "have a per-cycle discount applied."
        )
        parts.append(
            f"Average discount among discounted memberships: {_money(avg_disc)} per cycle."
        )
        if min_disc != max_disc:
            parts.append(f"Discount range: {_money(min_disc)} to {_money(max_disc)}.")
        n_full = total_active - n_disc
        parts.append(
            f"The remaining {n_full} active membership"
            f"{'s' if n_full != 1 else ''} {'are' if n_full != 1 else 'is'} "
            "billed at full price."
        )

    chunk = " ".join(parts)
    return {
        "doc_id":     f"{org_id}_memberships_discount_summary_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_discount_summary",
        "chunk_text": chunk,
        "metadata": {
            "active_total":        total_active,
            "discounted_count":    n_disc,
            "avg_discount":        round(avg_disc, 2),
            "min_discount":        round(min_disc, 2),
            "max_discount":        round(max_disc, 2),
            "as_of_date":          as_of.isoformat(),
        },
    }


def _build_churn_recap_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Powers Q13 (why did members cancel) and reinforces M-LQ5 (highest churn location).
    Always emits — "no cancellations in window" is itself a useful answer.

    Uses the canceled rows in `units` directly rather than relying on the monthly
    rollup, because units carry the per-customer detail (name, tenure, service).
    """
    canceled = [r for r in units if r.get("is_active") == 0 and r.get("canceled_at")]

    if not canceled:
        chunk = (
            f"Membership cancellation analysis as of {_fmt_date(as_of.isoformat())}: "
            "no cancellations have been recorded in the available history."
        )
        return {
            "doc_id":     f"{org_id}_memberships_churn_recap_{as_of.strftime('%Y_%m_%d')}",
            "doc_type":   "membership_churn_recap",
            "chunk_text": chunk,
            "metadata": {
                "cancellation_count":  0,
                "as_of_date":          as_of.isoformat(),
            },
        }

    # Group by month-of-cancel to identify clusters
    by_month: dict[str, list[dict]] = {}
    for r in canceled:
        month_key = r["canceled_at"][:7]   # 'YYYY-MM'
        by_month.setdefault(month_key, []).append(r)

    # The "cluster month" is the one with the most cancellations
    cluster_month, cluster_rows = max(
        by_month.items(), key=lambda kv: len(kv[1])
    )
    cluster_label = _month_label(cluster_month + "-01")

    # Location concentration in cluster
    cluster_locs = {r["location_id"] for r in cluster_rows}
    cluster_loc_str = ", ".join(_loc_name(l) for l in sorted(cluster_locs))
    one_location = len(cluster_locs) == 1
    one_loc_name = _loc_name(next(iter(cluster_locs))) if one_location else None

    parts = []
    total = len(canceled)
    parts.append(
        f"Membership cancellation analysis. Total cancellations across the "
        f"available history: {total}."
    )

    if len(by_month) == 1 or len(cluster_rows) == total:
        # Single concentrated event
        if one_location:
            parts.append(
                f"All {total} cancellations occurred at the {one_loc_name} location "
                f"in {cluster_label}:"
            )
        else:
            parts.append(
                f"All {total} cancellations occurred in {cluster_label} across "
                f"{cluster_loc_str}:"
            )
    else:
        parts.append(
            f"The largest cluster was {cluster_label} with "
            f"{len(cluster_rows)} cancellation"
            f"{'s' if len(cluster_rows) != 1 else ''}"
            + (f" all at the {one_loc_name} location" if one_location else "")
            + ":"
        )

    # List the cluster cancellations with detail
    for r in cluster_rows:
        name    = r.get("customer_name") or f"Customer {r['customer_id']}"
        service = r.get("service_name") or f"Service {r['service_id']}"
        tenure  = r.get("tenure_days") or 0
        parts.append(
            f"• {name} — {service}, {tenure} day{'s' if tenure != 1 else ''} tenure"
        )

    # Pattern detection
    cluster_services = [r.get("service_name") for r in cluster_rows if r.get("service_name")]
    if cluster_services:
        from collections import Counter
        svc_counts = Counter(cluster_services)
        top_svc, top_n = svc_counts.most_common(1)[0]
        if top_n >= 2 and top_n / len(cluster_rows) >= 0.5:
            parts.append(
                f"Dominant pattern: {top_n} of {len(cluster_rows)} cluster "
                f"cancellations were {top_svc} memberships."
            )

    avg_tenure_cluster = sum((r.get("tenure_days") or 0) for r in cluster_rows) / len(cluster_rows)
    if avg_tenure_cluster >= 180:
        parts.append(
            f"Average tenure of canceled members in the cluster: "
            f"{avg_tenure_cluster:.0f} days — these were established, "
            "long-tenured members rather than recent signups."
        )

    # Quiet months
    months_with_cancels = set(by_month.keys())
    as_of_month = as_of.strftime("%Y-%m")
    if as_of_month not in months_with_cancels:
        parts.append(
            f"No cancellations have occurred in {_month_label(as_of_month + '-01')}."
        )

    chunk = " ".join(parts) if "•" not in " ".join(parts) else "\n".join(parts)

    return {
        "doc_id":     f"{org_id}_memberships_churn_recap_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_churn_recap",
        "chunk_text": chunk,
        "metadata": {
            "cancellation_count":  total,
            "cluster_month":       cluster_month,
            "cluster_size":        len(cluster_rows),
            "cluster_locations":   sorted(cluster_locs),
            "as_of_date":          as_of.isoformat(),
        },
    }


def _build_business_overview_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Top-of-domain overview — a one-shot answer to many questions
    that need a few KPIs at once. Powers Q1, Q3, Q18, Q20 indirectly.
    """
    if not units:
        return None

    active   = [r for r in units if r.get("is_active") == 1]
    canceled = [r for r in units if r.get("is_active") == 0]
    if not active and not canceled:
        return None

    n_active = len(active)
    mrr      = sum(float(r.get("monthly_equivalent_revenue") or 0) for r in active)
    avg_ltv  = (sum(float(r.get("estimated_ltv") or 0) for r in active) / n_active) if n_active else 0
    used     = sum(1 for r in active if r.get("is_used") == 1)
    ghosts   = sum(1 for r in active if r.get("is_used") == 0)
    due_7    = sum(1 for r in active if r.get("is_due_in_7_days") == 1)
    react    = sum(1 for r in units  if r.get("is_reactivation") == 1)

    # Locations + services represented
    locs_active = sorted({r["location_id"] for r in active})
    svcs_active = sorted({r["service_id"]  for r in active})

    # Per-location active counts (for the closing summary)
    per_loc: dict[int, int] = {}
    for r in active:
        per_loc[r["location_id"]] = per_loc.get(r["location_id"], 0) + 1
    loc_summary = ", ".join(
        f"{_loc_name(l)} ({n} active)"
        for l, n in sorted(per_loc.items(), key=lambda kv: -kv[1])
    )

    parts = [
        f"Membership program overview as of {_fmt_date(as_of.isoformat())}:",
        f"• Active memberships: {n_active} (across {len(locs_active)} location"
        f"{'s' if len(locs_active) != 1 else ''}, {len(svcs_active)} service"
        f"{'s' if len(svcs_active) != 1 else ''})",
        f"• Monthly recurring revenue: {_money(mrr)} MRR",
        f"• Average lifetime value: {_money(avg_ltv)} per member",
        f"• Members actively using their membership: {used} of {n_active}"
        + (f" ({used / n_active * 100:.0f}%)" if n_active else ""),
    ]
    if ghosts > 0:
        parts.append(f"• Ghost memberships (active, never visited): {ghosts}")
    parts.append(f"• Memberships due for next charge in next 7 days: {due_7}")
    if react > 0:
        parts.append(
            f"• Reactivations (canceled and resigned within 90 days): {react}"
        )
    if canceled:
        parts.append(
            f"• Canceled memberships in the available history: {len(canceled)}"
        )
    parts.append(f"Distribution: {loc_summary}.")

    chunk = "\n".join(parts)
    return {
        "doc_id":     f"{org_id}_memberships_business_overview_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_business_overview",
        "chunk_text": chunk,
        "metadata": {
            "active_count":        n_active,
            "mrr":                 round(mrr, 2),
            "avg_ltv":             round(avg_ltv, 2),
            "used_count":          used,
            "ghost_count":         ghosts,
            "due_in_7_days":       due_7,
            "reactivation_count":  react,
            "canceled_total":      len(canceled),
            "as_of_date":          as_of.isoformat(),
        },
    }


def _build_signup_activity_doc(
    units: list[dict], monthly: list[dict], as_of: date, org_id: int,
) -> Optional[dict]:
    """
    Powers M-LQ4 (which location signed up the most last month) AND
    Q4/Q7 (signup trends, peak month) by pre-computing the cross-location
    signup comparison for the last 12 months.

    Cross-month + cross-location questions can't be answered reliably by
    cosine retrieval over individual location-month docs — top-K can land
    on the wrong location for the right month or vice versa. This rollup
    answers the comparison directly.
    """
    if not monthly:
        return None

    # Sort all monthly rows by date desc, take the 12 most recent months
    # (a "month" is a date string here, so lexicographic sort is correct).
    sorted_rows = sorted(monthly, key=lambda r: str(r["month_start"]), reverse=True)

    # Distinct months in descending order
    seen_months: list[str] = []
    for r in sorted_rows:
        m = r["month_start"] if isinstance(r["month_start"], str) else r["month_start"].isoformat()
        if m not in seen_months:
            seen_months.append(m)
        if len(seen_months) >= 12:
            break

    if not seen_months:
        return None

    # Group rows by (month, location) for easy lookup
    by_key: dict[tuple[str, int], dict] = {}
    for r in monthly:
        m = r["month_start"] if isinstance(r["month_start"], str) else r["month_start"].isoformat()
        by_key[(m, r["location_id"])] = r

    # Locations that appear in any of the 12 months
    locations_seen: set[int] = set()
    for m in seen_months:
        for r in monthly:
            mr = r["month_start"] if isinstance(r["month_start"], str) else r["month_start"].isoformat()
            if mr == m:
                locations_seen.add(r["location_id"])
    sorted_locs = sorted(locations_seen)

    # Build the per-month breakdown, oldest → newest for natural reading
    parts: list[str] = []
    parts.append(
        f"Recent membership signup activity by location (last "
        f"{len(seen_months)} month{'s' if len(seen_months) != 1 else ''} "
        f"through {_month_label(seen_months[0])}):"
    )

    # Track totals for the closing summary
    location_totals: dict[int, int] = {l: 0 for l in sorted_locs}
    monthly_totals: dict[str, int] = {}
    monthly_winners: dict[str, tuple[int, int]] = {}   # month → (loc_id, signups)

    for m in reversed(seen_months):   # oldest first for chronology
        signups_in_month: list[tuple[int, int]] = []   # (loc_id, signups)
        for loc_id in sorted_locs:
            row = by_key.get((m, loc_id))
            n = int(row.get("new_signups") or 0) if row else 0
            signups_in_month.append((loc_id, n))
            location_totals[loc_id] += n

        month_total = sum(n for _, n in signups_in_month)
        monthly_totals[m] = month_total

        if month_total == 0:
            parts.append(f"• {_month_label(m)} — 0 new signups across all locations")
            monthly_winners[m] = (-1, 0)
            continue

        # Sort within the month by signups desc
        signups_in_month.sort(key=lambda t: -t[1])
        winner_loc, winner_n = signups_in_month[0]
        monthly_winners[m] = (winner_loc, winner_n)

        breakdown = ", ".join(
            f"{_loc_name(l)} {n}" for l, n in signups_in_month
        )
        parts.append(
            f"• {_month_label(m)} — {month_total} total: {breakdown}"
        )

    # Closing summary: the most recent month + the peak month
    most_recent = seen_months[0]
    rec_winner_loc, rec_winner_n = monthly_winners[most_recent]
    if rec_winner_n > 0:
        parts.append(
            f"Most recent month ({_month_label(most_recent)}): "
            f"{_loc_name(rec_winner_loc)} led with {rec_winner_n} new "
            f"signup{'s' if rec_winner_n != 1 else ''}."
        )
    else:
        parts.append(
            f"Most recent month ({_month_label(most_recent)}): no new "
            f"signups at any location."
        )

    # Peak signup month across the window
    if monthly_totals:
        peak_month = max(monthly_totals, key=lambda m: monthly_totals[m])
        peak_total = monthly_totals[peak_month]
        if peak_total > 0:
            peak_winner_loc, peak_winner_n = monthly_winners[peak_month]
            parts.append(
                f"Peak signup month in the window: {_month_label(peak_month)} "
                f"with {peak_total} total signup{'s' if peak_total != 1 else ''} "
                f"(led by {_loc_name(peak_winner_loc)} with "
                f"{peak_winner_n})."
            )

    # Per-location totals across the window
    location_total_lines = ", ".join(
        f"{_loc_name(l)} {location_totals[l]}"
        for l in sorted(sorted_locs, key=lambda l: -location_totals[l])
    )
    parts.append(f"Total signups by location over the window: {location_total_lines}.")

    chunk = "\n".join(parts)

    return {
        "doc_id":     f"{org_id}_memberships_signup_activity_{as_of.strftime('%Y_%m_%d')}",
        "doc_type":   "membership_signup_activity",
        "chunk_text": chunk,
        "metadata": {
            "months_covered":   len(seen_months),
            "most_recent_month":      most_recent,
            "most_recent_winner_loc": rec_winner_loc if rec_winner_n > 0 else None,
            "most_recent_winner_n":   rec_winner_n,
            "peak_month":             max(monthly_totals, key=lambda m: monthly_totals[m]) if monthly_totals else None,
            "peak_total":             max(monthly_totals.values()) if monthly_totals else 0,
            "location_totals":        location_totals,
            "as_of_date":             as_of.isoformat(),
        },
    }


# Registry — order doesn't matter for retrieval, but keeping deterministic
_ROLLUP_BUILDERS = [
    _build_service_breakdown_doc,
    _build_tenure_ranking_doc,
    _build_discount_summary_doc,
    _build_churn_recap_doc,
    _build_signup_activity_doc,
    _build_business_overview_doc,
]


# ─────────────────────────────────────────────────────────────────────────────
#  Embed + upsert (replicates DocGenerator._store_doc)
# ─────────────────────────────────────────────────────────────────────────────

async def _store_one_doc(
    embedding_client: EmbeddingClient,
    vector_store:     VectorStore,
    tenant_id:        str,
    doc_id:           str,
    doc_type:         str,
    chunk_text:       str,
    period_start:     date | None,
    metadata:         dict,
    force:            bool,
) -> str:
    """Returns one of: 'created' | 'skipped' | 'failed'."""
    content_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()

    if not force:
        existing = await vector_store.get_doc_ids(tenant_id, DOMAIN, doc_type)
        if doc_id in existing:
            stored = await vector_store.get_doc_metadata(tenant_id, doc_id) or {}
            if stored.get("content_hash") == content_hash:
                return "skipped"

    full_meta = {**(metadata or {}), "content_hash": content_hash}
    try:
        vec = await embedding_client.embed(chunk_text)
        await vector_store.upsert(
            tenant_id=tenant_id,
            doc_id=doc_id,
            doc_domain=DOMAIN,
            doc_type=doc_type,
            chunk_text=chunk_text,
            embedding=vec,
            period_start=period_start,
            metadata=full_meta,
        )
    except Exception:
        logger.exception("memberships embed/upsert failed doc_id=%s", doc_id)
        return "failed"
    return "created"


# ─────────────────────────────────────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────────────────────────────────────

async def generate_membership_docs(
    org_id:           int,
    warehouse_rows:   dict,
    embedding_client: EmbeddingClient,
    vector_store:     VectorStore,
    force:            bool = False,
) -> dict:
    """
    Build, embed, and store memberships documents from MembershipsExtractor output.

    Args:
        org_id:           tenant business_id
        warehouse_rows:   dict from MembershipsExtractor.run() with keys
                          'units', 'monthly', 'as_of_date'
        embedding_client: produces vector embeddings
        vector_store:     pgvector wrapper
        force:            bypass content-hash skip detection

    Returns:
        {"docs_created": int, "docs_skipped": int, "docs_failed": int}
    """
    units   = warehouse_rows.get("units")   or []
    monthly = warehouse_rows.get("monthly") or []
    as_of   = warehouse_rows.get("as_of_date")
    if isinstance(as_of, str):
        as_of = date.fromisoformat(as_of)

    tenant_id = str(org_id)
    docs_created = docs_skipped = docs_failed = 0

    # ── Unit docs (one per subscription) ────────────────────────────────────
    as_of_suffix = as_of.strftime("%Y_%m_%d") if as_of else "current"
    for r in units:
        sub_id = r["subscription_id"]
        doc_id = f"{org_id}_memberships_unit_{sub_id}_{as_of_suffix}"
        chunk = _unit_chunk_text(r)
        meta = {
            "subscription_id":  sub_id,
            "location_id":      r["location_id"],
            "customer_id":      r["customer_id"],
            "customer_name":    r.get("customer_name"),
            "service_id":       r["service_id"],
            "service_name":     r.get("service_name"),
            "interval_bucket":  r.get("interval_bucket"),
            "is_active":        r.get("is_active"),
            "is_reactivation": r.get("is_reactivation"),
            "is_due_in_7_days": r.get("is_due_in_7_days"),
            "tenure_days":      r.get("tenure_days"),
            "monthly_equivalent_revenue": r.get("monthly_equivalent_revenue"),
            "as_of_date":       as_of.isoformat() if as_of else None,
        }
        st = await _store_one_doc(
            embedding_client, vector_store,
            tenant_id, doc_id, "membership_unit",
            chunk, as_of, meta, force,
        )
        if st == "created":
            docs_created += 1
        elif st == "skipped":
            docs_skipped += 1
        else:
            docs_failed += 1

    # ── Monthly summary docs (one per location-month) ───────────────────────
    for r in monthly:
        loc_id = r["location_id"]
        month_start_iso = r["month_start"]
        if not isinstance(month_start_iso, str):
            month_start_iso = month_start_iso.isoformat()
        month_ps = date.fromisoformat(month_start_iso)
        doc_id = f"{org_id}_memberships_monthly_loc_{loc_id}_{_ym_suffix(month_start_iso)}"
        chunk = _monthly_chunk_text(r)
        meta = {
            "location_id":         loc_id,
            "month_start":         month_start_iso,
            "active_at_month_end": r.get("active_at_month_end"),
            "mrr":                 r.get("mrr"),
            "new_signups":         r.get("new_signups"),
            "reactivations":       r.get("reactivations"),
            "cancellations":       r.get("cancellations"),
            "failed_charges":      r.get("failed_charges"),
            "mrr_mom_pct":         r.get("mrr_mom_pct"),
            "churn_rate_pct":      r.get("churn_rate_pct"),
        }
        st = await _store_one_doc(
            embedding_client, vector_store,
            tenant_id, doc_id, "membership_monthly_summary",
            chunk, month_ps, meta, force,
        )
        if st == "created":
            docs_created += 1
        elif st == "skipped":
            docs_skipped += 1
        else:
            docs_failed += 1

    # ── Rollup synthesis docs (one per builder, when applicable) ────────────
    rollups_emitted = 0
    if as_of is not None:
        for builder in _ROLLUP_BUILDERS:
            try:
                doc = builder(units, monthly, as_of, org_id)
            except Exception:
                logger.exception(
                    "memberships rollup builder failed: %s", builder.__name__,
                )
                docs_failed += 1
                continue
            if doc is None:
                continue
            st = await _store_one_doc(
                embedding_client, vector_store,
                tenant_id, doc["doc_id"], doc["doc_type"],
                doc["chunk_text"], as_of, doc["metadata"], force,
            )
            if st == "created":
                docs_created += 1
                rollups_emitted += 1
            elif st == "skipped":
                docs_skipped += 1
                rollups_emitted += 1
            else:
                docs_failed += 1

    logger.info(
        "memberships docs done org=%d — created=%d skipped=%d failed=%d "
        "(units=%d monthly=%d rollups=%d)",
        org_id, docs_created, docs_skipped, docs_failed,
        len(units), len(monthly), rollups_emitted,
    )

    return {
        "docs_created": docs_created,
        "docs_skipped": docs_skipped,
        "docs_failed":  docs_failed,
    }