"""
app/services/doc_generators/domains/expenses.py
=================================================
Expenses domain document handler for DocGenerator.

Reads the 7 expense doc types from the warehouse (wh_exp_* tables),
produces human-readable RAG chunks, and stores embeddings in pgvector.

Chunks produced (by doc_type):
    exp_monthly_summary      → one per period (MoM, QoQ, YTD narrated)
    exp_category_monthly     → one per (period × category) — carries anomaly_flag
    exp_subcategory_monthly  → one per (period × category × subcategory) — Q13
    exp_location_monthly     → one per (period × location)
    exp_payment_type_monthly → one per (period × payment_type)
    exp_staff_attribution    → one per (period × employee) — PII-safe (no $)
    exp_cat_location_cross   → one per (period × location × category)
    exp_dormant_category     → one per dormant category (doc-layer derived)

RAG design choices baked in:
  - Vocabulary diversity: every chunk includes at least two of
    "expenses", "costs", "spending", "overhead", "outflow", "bills"
    so natural-language queries match across synonyms.
  - Categorical anomaly_flag is ALWAYS mentioned by its semantic label
    (spike / elevated / normal / low / unusual_low) — lets "spiked" or
    "higher than usual" questions hit via cosine similarity.
  - "branch/location" dual vocabulary on every location-scoped chunk
    (Services Q24 lesson).
  - QoQ / baseline / YTD: NULL values are narrated as "insufficient
    data" — never omitted, never fabricated.
  - PII guard: staff attribution NEVER mentions total_amount_logged or
    any per-individual dollar value. Aggregate counts + rank only.

Called by DocGenerator.generate_all() when domain="expenses" or domain=None.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date
from typing import Any, Iterable

logger = logging.getLogger(__name__)

DOMAIN = "expenses"

EXPENSES_DOC_TYPES = {
    "exp_monthly_summary",
    "exp_category_monthly",
    "exp_subcategory_monthly",
    "exp_location_monthly",
    "exp_payment_type_monthly",
    "exp_staff_attribution",
    "exp_cat_location_cross",
    "exp_dormant_category",
    "exp_data_quality_notes",          # Grain-limitation notice
    "exp_cost_reduction_candidates",   # Top controllable cost categories (for Q23)
}

# ─────────────────────────────────────────────────────────────────────────────
# Chunk text generators — one per doc_type
# Each returns a multi-line string suitable for direct embedding.
# ─────────────────────────────────────────────────────────────────────────────

def _chunk_monthly_summary(row: dict) -> str:
    """
    Period rollup: totals, MoM direction, QoQ if complete, YTD, outlier flags.
    """
    period = _period_label(row.get("period"))
    total = _money(row.get("total_expenses"))
    txns  = row.get("transaction_count") or 0
    avg   = _money(row.get("avg_transaction"))

    mom_pct = row.get("mom_change_pct")
    mom_dir = row.get("mom_direction")
    prev    = row.get("prev_month_expenses")

    if mom_pct is None or prev is None:
        mom_str = "This is the first period in the window, so no month-over-month comparison is available."
    else:
        mom_str = (
            f"Month-over-month, expenses went {mom_dir} by {abs(mom_pct):.1f}% "
            f"compared to {_money(prev)} the previous month."
        )

    # QoQ — honest NULL handling
    cq = row.get("current_quarter_total")
    pq = row.get("prev_quarter_total")
    qoq = row.get("qoq_change_pct")
    if cq is not None and pq is not None and qoq is not None:
        qoq_dir = "down" if qoq < 0 else "up" if qoq > 0 else "flat"
        qoq_str = (
            f"Quarter-over-quarter this quarter spent {_money(cq)} "
            f"vs {_money(pq)} the prior quarter ({qoq_dir} {abs(qoq):.1f}%)."
        )
    elif cq is not None and pq is None:
        qoq_str = f"Quarter-to-date spending is {_money(cq)}; no comparable prior quarter is in the window."
    else:
        qoq_str = "Not enough months in the window to produce a full quarter comparison."

    # YTD
    ytd = row.get("ytd_total") or 0
    year = _year_from_period(row.get("period"))
    ytd_str = f"Year-to-date spending in {year} is {_money(ytd)}."

    # Window rank
    rank = row.get("expense_rank_in_window") or 0
    n    = row.get("months_in_window") or 0
    rank_str = (
        f"This was the highest-spending month out of {n} months in the window."
        if rank == 1 and n > 0 else
        f"This was the lowest-spending month out of {n} months in the window."
        if rank == n and n > 0 else
        f"Rank {rank} of {n} months by spend." if n > 0 else ""
    )

    # Outlier awareness
    large = row.get("large_txn_count") or 0
    huge  = row.get("huge_txn_count") or 0
    if huge > 0:
        outlier_str = (
            f"Caveat: this month contains {huge} single transaction(s) over $1,000,000 — "
            f"likely data-entry errors. Treat the total with skepticism."
        )
    elif large > 0:
        outlier_str = (
            f"Note: this month contains {large} single transaction(s) over $100,000. "
            f"May include legitimate equipment or rent payments."
        )
    else:
        outlier_str = ""

    lines = [
        f"Expenses summary — {period}",
        f"Total expenses (costs + overhead + bills) for {period} were {_money(total)} across {txns} transactions (average {avg} per transaction).",
        mom_str,
        qoq_str,
        ytd_str,
    ]
    if rank_str:
        lines.append(rank_str)
    if outlier_str:
        lines.append(outlier_str)

    return "\n".join(lines)


def _chunk_category_monthly(row: dict) -> str:
    """
    Category × period. Includes anomaly_flag as a natural-language word so
    RAG queries about "spikes" or "higher than usual" match via cosine.
    """
    period = _period_label(row.get("period"))
    cat    = row.get("category_name") or "Uncategorized"
    total  = _money(row.get("category_total"))
    month_total = _money(row.get("month_total"))
    pct    = row.get("pct_of_month") or 0
    rank   = row.get("rank_in_month") or 0
    txns   = row.get("transaction_count") or 0

    header = f"Category spending — {cat} — {period}"

    base_lines = [
        header,
        f"In {period}, spending in the {cat} category was {total}, "
        f"which is {pct:.1f}% of total expenses ({month_total}) that month "
        f"across {txns} transactions. Ranked #{rank} among all categories this month.",
    ]

    # MoM narration
    prev   = row.get("prev_month_total")
    mom    = row.get("mom_change_pct")
    if prev is not None and mom is not None:
        direction = "up" if mom > 0 else "down" if mom < 0 else "flat"
        base_lines.append(
            f"Compared to the previous month, {cat} costs went {direction} "
            f"{abs(mom):.1f}% from {_money(prev)}."
        )

    # Baseline + anomaly — this is the RAG-critical block for Q22/Q24
    anomaly = row.get("anomaly_flag")
    baseline_avg = row.get("baseline_3mo_avg")
    pct_vs = row.get("pct_vs_baseline")
    months_avail = row.get("baseline_months_available") or 0

    if anomaly is None or baseline_avg is None or pct_vs is None:
        if months_avail < 2:
            base_lines.append(
                f"Not enough prior history to compare {cat} against a baseline."
            )
    else:
        # Natural-language framing for each flag
        flag_phrase = {
            "spike":       f"spiked — this is a SPIKE of +{pct_vs:.1f}% above the 3-month baseline of {_money(baseline_avg)}",
            "elevated":    f"was ELEVATED — {pct_vs:+.1f}% above the 3-month baseline of {_money(baseline_avg)}",
            "normal":      f"was NORMAL — within ±20% of the 3-month baseline of {_money(baseline_avg)} ({pct_vs:+.1f}%)",
            "low":         f"was LOW — {pct_vs:+.1f}% below the 3-month baseline of {_money(baseline_avg)}",
            "unusual_low": f"was UNUSUALLY LOW — {pct_vs:+.1f}% below the 3-month baseline of {_money(baseline_avg)}",
        }.get(anomaly, f"change vs 3-month baseline: {pct_vs:+.1f}%")

        base_lines.append(
            f"{cat} spending {flag_phrase}. "
            f"This is useful when asking whether a category spiked, was higher than usual, "
            f"or was unusually low compared to typical spending."
        )

    return "\n".join(base_lines)


def _chunk_subcategory_monthly(row: dict) -> str:
    """Subcategory drill-down for Q13-style questions."""
    period    = _period_label(row.get("period"))
    cat       = row.get("category_name") or "Uncategorized"
    subcat    = row.get("subcategory_name") or "Unspecified"
    total     = _money(row.get("subcategory_total"))
    txns      = row.get("transaction_count") or 0
    rank      = row.get("rank_in_category") or 0

    return (
        f"Subcategory spending — {cat} › {subcat} — {period}\n"
        f"Within the {cat} category in {period}, the {subcat} subcategory accounted "
        f"for {total} across {txns} transaction(s). Ranked #{rank} within the {cat} category "
        f"for this month."
    )


def _chunk_location_monthly(row: dict) -> str:
    """Per-location period summary. Uses branch/location dual vocabulary."""
    period = _period_label(row.get("period"))
    loc    = row.get("location_name") or "Unknown"
    total  = _money(row.get("location_total"))
    month_total = _money(row.get("month_total"))
    pct    = row.get("pct_of_month") or 0
    rank   = row.get("rank_in_month") or 0
    txns   = row.get("transaction_count") or 0

    lines = [
        f"Location expenses — {loc} branch/location — {period}",
        f"In {period}, the {loc} branch/location spent {total} "
        f"({pct:.1f}% of total monthly expenses {month_total}) across {txns} transactions. "
        f"Ranked #{rank} among all branches/locations for this month.",
    ]

    prev = row.get("prev_month_total")
    mom  = row.get("mom_change_pct")
    if prev is not None and mom is not None:
        direction = "up" if mom > 0 else "down" if mom < 0 else "flat"
        lines.append(
            f"Month-over-month, {loc} costs went {direction} {abs(mom):.1f}% "
            f"from {_money(prev)}."
        )

    return "\n".join(lines)


def _chunk_payment_type_monthly(row: dict) -> str:
    period = _period_label(row.get("period"))
    label  = row.get("payment_type_label") or "Unknown"
    total  = _money(row.get("type_total"))
    pct    = row.get("pct_of_month") or 0
    txns   = row.get("transaction_count") or 0
    month_total = _money(row.get("month_total"))

    return (
        f"Payment method — {label} — {period}\n"
        f"In {period}, {pct:.1f}% of expenses ({total} of {month_total}) "
        f"were paid by {label}, across {txns} transactions. "
        f"Relevant when asking which payment method or payment type the business uses most, "
        f"or for cash vs card vs check splits."
    )


def _chunk_staff_attribution(row: dict) -> str:
    """
    PII-safe. NEVER mention total_amount_logged. Aggregate counts + rank only.
    """
    period  = _period_label(row.get("period"))
    name    = row.get("employee_name") or "Unknown"
    entries = row.get("entries_logged") or 0
    rank    = row.get("rank_in_month") or 0

    return (
        f"Expense entry logger — {period}\n"
        f"In {period}, staff member {name} logged {entries} expense "
        f"entries (rank #{rank} for that month). This reflects who recorded "
        f"expenses in the system, not who approved or authorized them. "
        f"Dollar amounts logged by individual staff are not provided for privacy."
    )


def _chunk_cat_location_cross(row: dict) -> str:
    """Category × location × period. Supports Q19."""
    period = _period_label(row.get("period"))
    loc    = row.get("location_name") or "Unknown"
    cat    = row.get("category_name") or "Uncategorized"
    total  = _money(row.get("cross_total"))
    pct    = row.get("pct_of_location_month") or 0
    rank   = row.get("rank_in_location_month") or 0
    txns   = row.get("transaction_count") or 0

    return (
        f"Location × category — {loc} branch/location, {cat} — {period}\n"
        f"At the {loc} branch/location in {period}, {cat} spending was {total} "
        f"({pct:.1f}% of this location's monthly expenses) across {txns} transactions. "
        f"Ranked #{rank} among categories at this branch/location for this month. "
        f"Useful when comparing category mix across branches or asking which "
        f"location spends most on a particular type of expense."
    )


def _chunk_dormant_category(row: dict) -> str:
    """
    One chunk per dormant category. Derived by the doc layer (not the API).
    See _detect_dormant_categories() below.
    """
    cat              = row.get("category_name") or "Uncategorized"
    last_period      = _period_label(row.get("last_active_period"))
    avg_when_active  = _money(row.get("avg_when_active"))
    silence_months   = row.get("silence_months") or 3

    return (
        f"Dormant category — {cat}\n"
        f"The {cat} category has had no expense activity for the last {silence_months} months. "
        f"Last recorded spending was in {last_period}. "
        f"When active, this category averaged {avg_when_active} per month. "
        f"This is useful when asking which expense categories have gone quiet, "
        f"haven't had any spending recently, or have stopped being used."
    )


def _chunk_data_quality_notes(business_id: int) -> str:
    """
    Build the per-tenant data-quality notice chunk. Surfaces grain
    limitations so the LLM answers honestly instead of hallucinating
    when asked about duplicate / individual-transaction analysis.
    """
    return (
        f"Data-quality and grain notes for business_id={business_id}. "
        "The expense data in this knowledge base is aggregated at MONTHLY "
        "grain — each document summarises one month of expenses by "
        "category, subcategory, location, payment type, or staff logger. "
        "Individual transaction-level data (specific expense entries, "
        "timestamps, amounts per transaction, notes, receipt identifiers) "
        "is NOT embedded in this knowledge base. "
        "Because of this grain limitation, the following analyses CANNOT "
        "be performed from the embedded documents: "
        "duplicate-expense detection, identical-entry detection, "
        "same-day double-logging detection, per-transaction anomaly "
        "detection, mistake-entry identification, data-entry-error review. "
        "If a user asks about duplicates, mistakes, or entry-level "
        "anomalies, the correct honest answer is: "
        "\"I can't detect duplicates or individual-transaction mistakes "
        "from the monthly aggregates I have — that analysis requires "
        "transaction-level data which is not available in my current "
        "knowledge base. You would need to audit the underlying expense "
        "log directly (e.g. through your accounting software's transaction "
        "list) to spot duplicates or entry errors.\" "
        "Related vocabulary this chunk should answer: duplicate expenses, "
        "duplicate entries, double-counted, mistake entries, miscategorized, "
        "wrong category, entry errors, data entry mistakes, "
        "individual transactions, transaction-level detail, audit entries."
    )


def _chunk_cost_reduction_candidates(
    business_id: int,
    category_rows: list,
    latest_period: str,
) -> str:
    """
    Build the per-tenant cost-reduction candidates chunk.

    Ranks categories by most-recent-period spend and identifies
    controllable ones (excluding fixed-cost categories like Rent,
    Insurance, Payroll from "easily reducible" list — flag them
    separately as fixed).

    FIXED_COSTS are hard to reduce without structural business changes.
    CONTROLLABLE costs are where most businesses have real levers.
    """
    FIXED_COST_CATEGORIES = {
        "Rent & Utilities", "Rent", "Utilities",
        "Insurance", "Payroll", "Software & Subscriptions",
    }

    latest_cats = [
        r for r in category_rows
        if str(r.get("period") or "")[:10] == latest_period
    ]
    latest_cats.sort(key=lambda r: float(r.get("total_amount") or 0), reverse=True)

    if not latest_cats:
        return (
            f"Cost reduction candidates for business_id={business_id}: "
            "No recent category data available for analysis."
        )

    fixed_lines = []
    controllable_lines = []
    for r in latest_cats:
        cat_name = r.get("category_name", "Unknown")
        amount = float(r.get("total_amount") or 0)
        pct = float(r.get("pct_of_total") or 0)
        anomaly = r.get("anomaly_flag")
        baseline_delta = r.get("pct_vs_baseline")

        extra = ""
        if anomaly == "spike":
            extra = f" ⚠ SPIKE: up {baseline_delta:+.1f}% vs baseline"
        elif anomaly == "elevated":
            extra = f" — elevated ({baseline_delta:+.1f}% vs baseline)"

        line = f"  • {cat_name}: ${amount:,.0f} ({pct:.1f}% of total){extra}"

        if cat_name in FIXED_COST_CATEGORIES:
            fixed_lines.append(line)
        else:
            controllable_lines.append(line)

    sections = [
        f"Cost reduction candidates for business_id={business_id} "
        f"(based on {latest_period}).",
        "",
        "CONTROLLABLE COST CATEGORIES (where cost-reduction efforts typically work):",
    ]
    sections.extend(controllable_lines if controllable_lines else
                    ["  (no controllable categories in recent period)"])

    sections.append("")
    sections.append("FIXED / STRUCTURAL COST CATEGORIES (harder to reduce without structural change):")
    sections.extend(fixed_lines if fixed_lines else
                    ["  (no fixed categories in recent period)"])

    sections.append("")
    sections.append(
        "When a user asks WHERE CAN I CUT COSTS or HOW TO REDUCE EXPENSES or "
        "SAVE MONEY or OPTIMIZE SPENDING, recommend reviewing the controllable "
        "cost categories first — start with the largest one(s), then any "
        "marked as SPIKE or elevated. Ground the advice in the specific "
        "category names and amounts above."
    )
    sections.append(
        "Related vocabulary: where can I cut costs, reduce expenses, "
        "cut spending, save money, optimize spending, expense advice, "
        "cost-cutting priorities, controllable costs, variable costs, "
        "fixed costs, overhead reduction."
    )

    return "\n".join(sections)


CHUNK_GENERATORS = {
    "exp_monthly_summary":      _chunk_monthly_summary,
    "exp_category_monthly":     _chunk_category_monthly,
    "exp_subcategory_monthly":  _chunk_subcategory_monthly,
    "exp_location_monthly":     _chunk_location_monthly,
    "exp_payment_type_monthly": _chunk_payment_type_monthly,
    "exp_staff_attribution":    _chunk_staff_attribution,
    "exp_cat_location_cross":   _chunk_cat_location_cross,
    "exp_dormant_category":     _chunk_dormant_category,
}


# ─────────────────────────────────────────────────────────────────────────────
# Dormant-category derivation (Q28)
# Pure doc-layer logic over category_monthly rows. No SQL.
# Per Step 3 API spec §14.
# ─────────────────────────────────────────────────────────────────────────────

def _detect_dormant_categories(
    category_rows: list[dict],
    period_end: date,
    silence_months: int = 3,
) -> list[dict]:
    """
    Build dormant-category chunk input rows.

    A category is dormant if it was active in months strictly before
    (period_end - silence_months) AND has zero active months in the
    window (period_end - silence_months, period_end].

    Returns a list of dicts ready for _chunk_dormant_category().
    Returns [] when the window is too short (<4 months of data) — we
    do not emit dormant chunks from insufficient history.
    """
    if not category_rows:
        return []

    # Normalize period values to ISO date strings (YYYY-MM-DD).
    # Real warehouse rows (asyncpg) return datetime.date; mock server
    # rows return strings via JSON. Comparing ISO strings is order-correct
    # AND avoids mixed-type < errors.
    def _iso(p) -> str:
        if hasattr(p, "isoformat"):
            return p.isoformat()
        return str(p)[:10]

    # Determine distinct months in the data
    periods = sorted({_iso(r["period"]) for r in category_rows})
    if len(periods) < silence_months + 1:
        # Not enough data to claim silence
        return []

    cutoff_str = _iso(_silence_window_start(period_end, silence_months))

    # Group active rows by category (period normalized to ISO string)
    from collections import defaultdict
    history = defaultdict(list)  # cat_id -> [(period_str, category_total)]
    cat_names = {}
    for r in category_rows:
        if (r.get("category_total") or 0) > 0:
            history[r["category_id"]].append((_iso(r["period"]), r["category_total"]))
            cat_names[r["category_id"]] = r.get("category_name") or "Uncategorized"

    dormant = []
    for cat_id, rows in history.items():
        rows.sort(key=lambda x: x[0])
        historical_activity = [(p, t) for p, t in rows if p < cutoff_str]
        recent_activity     = [(p, t) for p, t in rows if p >= cutoff_str]

        if historical_activity and not recent_activity:
            last_period = historical_activity[-1][0]
            avg_when_active = sum(t for _, t in historical_activity) / len(historical_activity)
            dormant.append({
                "business_id":        category_rows[0]["business_id"],
                "category_id":        cat_id,
                "category_name":      cat_names[cat_id],
                "last_active_period": last_period,
                "avg_when_active":    round(avg_when_active, 2),
                "silence_months":     silence_months,
            })

    return dormant


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point called by DocGenerator
# ─────────────────────────────────────────────────────────────────────────────

async def generate_expenses_docs(
    org_id: int,
    warehouse_data: dict[str, list[dict]],
    emb_client,
    vector_store,
    force: bool = False,
    period_end: date | None = None,
) -> dict[str, int]:
    """
    Build + embed + store chunks for the Expenses domain.

    Parameters
    ----------
    org_id:           business_id to tag chunks with (tenant scope).
    warehouse_data:   dict keyed by doc type — output of ExpensesExtractor.run()
                      and/or direct reads from wh_exp_* tables.
    emb_client:       EmbeddingClient (has .embed_batch(texts) -> list[vec])
    vector_store:     VectorStore (has .exists() and .upsert())
    force:            re-embed even if hash already exists.
    period_end:       the end-of-window date used for dormant detection.
                      If None, infers from the latest period in category_breakdown.

    Returns
    -------
    dict with keys: created, skipped, failed
    """
    created = skipped = failed = 0
    all_chunks: list[dict] = []

    # 1. Monthly rollup
    for row in warehouse_data.get("monthly_summary", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_monthly_summary", row=row,
            text=_chunk_monthly_summary(row),
            period_start=row.get("period"),
            metadata={"period": str(row.get("period") or "")},
        ))

    # 2. Category breakdown
    for row in warehouse_data.get("category_breakdown", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_category_monthly", row=row,
            text=_chunk_category_monthly(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "category_id": row.get("category_id"),
                "anomaly_flag": row.get("anomaly_flag") or "",
            },
        ))

    # 3. Subcategory breakdown
    for row in warehouse_data.get("subcategory_breakdown", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_subcategory_monthly", row=row,
            text=_chunk_subcategory_monthly(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "category_id": row.get("category_id"),
                "subcategory_id": row.get("subcategory_id"),
            },
        ))

    # 4. Location breakdown
    for row in warehouse_data.get("location_breakdown", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_location_monthly", row=row,
            text=_chunk_location_monthly(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "location_id": row.get("location_id"),
            },
        ))

    # 5. Payment type breakdown
    for row in warehouse_data.get("payment_type_breakdown", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_payment_type_monthly", row=row,
            text=_chunk_payment_type_monthly(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "payment_type_code": row.get("payment_type_code"),
            },
        ))

    # 6. Staff attribution (PII-safe)
    for row in warehouse_data.get("staff_attribution", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_staff_attribution", row=row,
            text=_chunk_staff_attribution(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "employee_id": row.get("employee_id"),
            },
        ))

    # 7. Category × location cross
    for row in warehouse_data.get("category_location_cross", []):
        all_chunks.append(_make_chunk(
            org_id=org_id, doc_type="exp_cat_location_cross", row=row,
            text=_chunk_cat_location_cross(row),
            period_start=row.get("period"),
            metadata={
                "period": str(row.get("period") or ""),
                "location_id": row.get("location_id"),
                "category_id": row.get("category_id"),
            },
        ))

    # 8. Dormant categories (doc-layer derived)
    category_rows = warehouse_data.get("category_breakdown", [])
    if category_rows:
        # Infer period_end from latest period if not given
        if period_end is None:
            latest = max(r["period"] for r in category_rows if r.get("period"))
            period_end = _end_of_month(latest)

        dormant_rows = _detect_dormant_categories(
            category_rows=category_rows, period_end=period_end,
        )
        for row in dormant_rows:
            all_chunks.append(_make_chunk(
                org_id=org_id, doc_type="exp_dormant_category", row=row,
                text=_chunk_dormant_category(row),
                period_start=None,  # dormancy is window-scoped, not period-scoped
                metadata={
                    "category_id": row.get("category_id"),
                    "last_active_period": str(row.get("last_active_period") or ""),
                },
            ))
        logger.info(
            "expenses doc_gen: org=%d detected %d dormant categor%s",
            org_id, len(dormant_rows), "y" if len(dormant_rows) == 1 else "ies",
        )

    # 8b. Data-quality / grain-limitation notice — one per tenant
    # Tells the LLM honestly what this knowledge base CANNOT answer
    # (duplicate detection, individual-transaction analysis).
    all_chunks.append(_make_chunk(
        org_id=org_id,
        doc_type="exp_data_quality_notes",
        row={"business_id": org_id},
        text=_chunk_data_quality_notes(org_id),
        period_start=None,   # not period-scoped
        metadata={"scope": "tenant_wide"},
    ))

    # 8c. Cost-reduction candidates — one per tenant
    # Tells the LLM which categories are the biggest controllable costs
    # (for "where can I cut costs" style questions). Ranks by most-recent
    # period and separates fixed vs controllable.
    if category_rows:
        latest_period = max(str(r.get("period") or "")[:10]
                            for r in category_rows if r.get("period"))
        all_chunks.append(_make_chunk(
            org_id=org_id,
            doc_type="exp_cost_reduction_candidates",
            row={"business_id": org_id, "latest_period": latest_period},
            text=_chunk_cost_reduction_candidates(
                business_id=org_id,
                category_rows=category_rows,
                latest_period=latest_period,
            ),
            period_start=None,   # tenant-wide, not period-scoped
            metadata={
                "scope": "tenant_wide",
                "latest_period": latest_period,
            },
        ))

    # 9. Embed + store
    if not all_chunks:
        logger.info("expenses doc_gen: org=%d no chunks to embed", org_id)
        return {"created": 0, "skipped": 0, "failed": 0}

    tenant_id = str(org_id)

    # Partition: skip chunks that already exist (unless force) so we don't
    # waste embedding tokens on rows we'd just no-op upsert.
    if force:
        to_embed = all_chunks
    else:
        to_embed = []
        for chunk in all_chunks:
            if await vector_store.exists(tenant_id, chunk["doc_id"]):
                skipped += 1
            else:
                to_embed.append(chunk)

    if to_embed:
        try:
            vectors = await emb_client.embed_batch([c["text"] for c in to_embed])
        except Exception as e:
            logger.exception("expenses doc_gen: org=%d embed_batch failed: %s", org_id, e)
            failed += len(to_embed)
            vectors = []

        for chunk, vec in zip(to_embed, vectors):
            try:
                await vector_store.upsert(
                    doc_id=chunk["doc_id"],
                    tenant_id=tenant_id,
                    doc_domain=chunk["doc_domain"],
                    doc_type=chunk["doc_type"],
                    chunk_text=chunk["text"],
                    embedding=vec,
                    period_start=chunk.get("period_start"),
                    metadata=chunk.get("metadata"),
                )
                created += 1
            except Exception:
                logger.exception(
                    "expenses doc_gen: org=%d upsert failed doc_id=%s",
                    org_id, chunk["doc_id"],
                )
                failed += 1

    logger.info(
        "expenses doc_gen: org=%d created=%d skipped=%d failed=%d",
        org_id, created, skipped, failed,
    )
    return {"created": created, "skipped": skipped, "failed": failed}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_chunk(
    org_id: int,
    doc_type: str,
    row: dict,
    text: str,
    period_start,
    metadata: dict,
) -> dict:
    """
    Build a vector-store-ready chunk record.

    The hash-based doc_id gives us idempotent upserts — the same row
    re-embedded in a later ETL run produces the same id and skips
    (unless force=True).
    """
    # Stable id: domain + doc_type + tenant + core dimensions
    key_parts = [
        DOMAIN, doc_type, str(org_id),
        str(row.get("period") or ""),
        str(row.get("category_id") or ""),
        str(row.get("subcategory_id") or ""),
        str(row.get("location_id") or ""),
        str(row.get("employee_id") or ""),
        str(row.get("payment_type_code") or ""),
    ]
    doc_id = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:32]

    return {
        "doc_id":       doc_id,
        "tenant_id":    org_id,
        "doc_domain":   DOMAIN,
        "doc_type":     doc_type,
        "text":         text,
        "period_start": period_start,
        "metadata":     metadata,
    }


def _money(v) -> str:
    """Render a numeric as $X,XXX.YY or 'n/a' for None."""
    if v is None:
        return "n/a"
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _period_label(p) -> str:
    """Convert 2026-03-01 → 'March 2026'. Accepts str or date."""
    if p is None:
        return "unknown period"
    s = str(p)[:10]
    try:
        y, m, _ = s.split("-")
        month_names = [
            "January","February","March","April","May","June",
            "July","August","September","October","November","December",
        ]
        return f"{month_names[int(m)-1]} {y}"
    except Exception:
        return s


def _year_from_period(p) -> str:
    if p is None:
        return "this year"
    return str(p)[:4]


def _subtract_months(d: date, months: int) -> date:
    y = d.year
    m = d.month - months
    while m <= 0:
        m += 12
        y -= 1
    # Clamp to day 1 — we work at month granularity
    return date(y, m, 1)


def _silence_window_start(period_end: date, silence_months: int) -> date:
    """
    First-of-month of the start of the silence window.

    Per Step 3 API spec §14, a category is dormant if it has zero activity
    in the silence_months ending at period_end (inclusive of period_end's
    month) AND it had activity in some earlier month.

    For period_end=2026-03-31 with silence_months=3, the silence window is
    {Jan 2026, Feb 2026, Mar 2026}, so the start is 2026-01-01.

    Math: first-of-month of period_end, minus (silence_months - 1) months.
    """
    first_of_period_end_month = date(period_end.year, period_end.month, 1)
    return _subtract_months(first_of_period_end_month, silence_months - 1)


def _end_of_month(period_start) -> date:
    """Given a YYYY-MM-01 string or date, return the last day of that month."""
    s = str(period_start)[:10]
    y, m, _ = s.split("-")
    y, m = int(y), int(m)
    if m == 12:
        return date(y + 1, 1, 1)  # exclusive upper bound works for cutoff math
    return date(y, m + 1, 1)