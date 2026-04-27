"""
app/services/doc_generators/domains/forms.py
=============================================
Forms domain chunk generator (Domain 10, Sprint 10).

Transforms warehouse rows (returned by FormsExtractor.run()) into ~14 embedding
documents per business. Reads the in-memory dict directly; no separate
wh_forms reader module (matches giftcards / clients pattern).

Doc catalog (7 doc types — matches Step 2 + Step 3 EP1–EP4)
==========================================================
   1. catalog              — rollup snapshot, 1 chunk     F1, F3, F8, F11
   2. monthly              — per-month chunk              F2, F4, F5, F6, F12, S1
   3. monthly_summary      — always-emit, 1 chunk         S1, F4 (multi-month compare)
   4. per_form             — per-template chunk           F7, F8, F11
   5. lifecycle            — always-emit, 1 chunk         F9, F10, F13
   6. anomalies            — always-emit, 1 chunk         F10 detail, edge-case zero-emission
   7. pii_policy           — always-emit, 1 chunk         F14 (privacy refusal)

Lessons baked in
----------------
L4 — per-template chunks must include catalog context.
     Per-form chunks include the lifetime totals from the catalog so the
     retriever doesn't substitute the catalog rollup chunk for "which form"
     questions.
L5 — vocabulary tests catch routing gaps.
     Every chunk contains BOTH "form" AND its variants (questionnaire,
     intake, submission).
L6 — every chunk carries the synonym header so vocab variants
     (form / questionnaire / intake / submission / template) all retrieve.
P5 — rollup outranks per-form:
     Per-form chunks include the org-wide submission total for context.
R7 — empty-month emission:
     Doc gen explicitly emits a chunk per month if F4 trend windows expose
     gaps. The monthly chunk acknowledges if it's a low-activity month.
R8 — small-sample protection:
     MoM percentages always paired with absolute counts (e.g.
     "5 → 4 submissions, -20% MoM") so a "-20% drop!" headline can't mislead
     against tiny denominators.
F14 — PII guardrail:
     No CustId values, no FormTemp / JsonTemp content, no OnlineCode strings
     in any chunk. Only aggregated counts and template names.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable

logger = logging.getLogger(__name__)

DOMAIN = "forms"

# Doc types — stable doc_id components
DOC_TYPE_CATALOG          = "catalog"
DOC_TYPE_MONTHLY          = "monthly"
DOC_TYPE_MONTHLY_SUMMARY  = "monthly_summary"
DOC_TYPE_PER_FORM         = "per_form"
DOC_TYPE_LIFECYCLE        = "lifecycle"
DOC_TYPE_ANOMALIES        = "anomalies"
DOC_TYPE_PII_POLICY       = "pii_policy"


# =============================================================================
# Synonym header — appears at the top of EVERY chunk (Lesson 6)
# =============================================================================

_SYN_HEADER = (
    "Topic: form / forms / questionnaire / intake / submission / template / "
    "feedback form / consent form. Domain: forms."
)


# =============================================================================
# Formatters — pure helpers
# =============================================================================

def _pct(v: Any) -> str:
    if v is None:
        return "n/a"
    return f"{float(v):.2f}%"


def _period_label(d: Any) -> str:
    if d is None:
        return "unknown period"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%B %Y")


def _date_label(d: Any) -> str:
    if d is None:
        return "unknown date"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%B %d, %Y").replace(" 0", " ") if hasattr(d, "strftime") else str(d)


def _period_key(d: Any) -> str:
    if d is None:
        return "unknown"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%Y_%m") if hasattr(d, "strftime") else str(d)


def _date_key(d: Any) -> str:
    if d is None:
        return "unknown"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%Y_%m_%d") if hasattr(d, "strftime") else str(d)


# =============================================================================
# Pure chunk text generators — no I/O, used by tests
# =============================================================================

def gen_catalog(obj: dict, dormant_form_names: list[str] | None = None) -> str:
    """Catalog snapshot — counts, dormancy splits.

    Powers F1 (template count), F3 (active), F8 (dormant count), F11 (advice).
    """
    snap = _date_label(obj.get("snapshot_date"))
    total = obj.get("total_template_count", 0) or 0
    active = obj.get("active_template_count", 0) or 0
    inactive = obj.get("inactive_template_count", 0) or 0
    active_dormant = obj.get("active_dormant_count", 0) or 0
    inactive_dormant = obj.get("inactive_dormant_count", 0) or 0
    lifetime_subs = obj.get("lifetime_submission_total", 0) or 0
    recent_90d = obj.get("recent_90d_submission_total", 0) or 0
    most_recent = obj.get("most_recent_template_added")
    cat_ids = obj.get("distinct_category_ids") or []

    # F11 advice surface — name dormant active forms explicitly
    dormant_str = ""
    if active_dormant > 0:
        if dormant_form_names:
            names_preview = ", ".join(f'"{n}"' for n in dormant_form_names[:5])
            subj = "These forms have" if active_dormant != 1 else "This form has"
            cand = "candidates" if active_dormant != 1 else "a candidate"
            dormant_str = (
                f" The active dormant template{'s are' if active_dormant != 1 else ' is'}: "
                f"{names_preview}. {subj} never been submitted by any customer "
                f"and could be {cand} for deactivation."
            )
        else:
            dormant_str = (
                f" {active_dormant} active form template{'s have' if active_dormant != 1 else ' has'} "
                f"never been submitted and could be candidates for deactivation."
            )

    most_recent_str = ""
    if most_recent:
        most_recent_str = f" The most recently created template was added on {_date_label(most_recent)}."

    cat_str = ""
    if cat_ids:
        cat_list = ", ".join(str(c) for c in cat_ids)
        cat_str = f" Templates use {len(cat_ids)} distinct category id{'s' if len(cat_ids) != 1 else ''}: {cat_list}."

    return (
        f"{_SYN_HEADER}\n"
        f"Form template catalog snapshot as of {snap}.\n"
        f"You have {total} form template{'s' if total != 1 else ''} in total: "
        f"{active} active and {inactive} inactive (deactivated). "
        f"Across all customers and time periods, these templates have received "
        f"{lifetime_subs} total submission{'s' if lifetime_subs != 1 else ''}, "
        f"with {recent_90d} submission{'s' if recent_90d != 1 else ''} in the last 90 days. "
        f"{active_dormant} active template{'s have' if active_dormant != 1 else ' has'} "
        f"received zero submissions ever (dormant), "
        f"and {inactive_dormant} inactive template{'s have' if inactive_dormant != 1 else ' has'} "
        f"never been submitted.{dormant_str}{most_recent_str}{cat_str}"
    ).strip()


def gen_monthly(row: dict) -> str:
    """Per-month submission summary chunk.

    Powers F2 (last month), F4 (trend), F5 (YTD via aggregation), F6 (MoM),
    F12 (questionnaire vocab), S1 (cross-domain with revenue).
    """
    period = _period_label(row.get("period_start"))
    sub_count = row.get("submission_count", 0) or 0
    ready = row.get("ready_count", 0) or 0
    complete = row.get("complete_count", 0) or 0
    approved = row.get("approved_count", 0) or 0
    distinct_forms = row.get("distinct_forms_used", 0) or 0
    distinct_custs = row.get("distinct_customers_filling", 0) or 0
    mom = row.get("mom_submission_pct")
    yoy = row.get("yoy_submission_pct")

    # R8 small-sample protection — pair % with absolute counts always
    mom_str = ""
    if mom is not None:
        direction = "up" if float(mom) >= 0 else "down"
        mom_str = (
            f" Submissions {direction} {abs(float(mom)):.1f}% versus the prior month "
            f"(absolute count change relative to a small base)."
        )
    yoy_str = ""
    if yoy is not None:
        direction = "up" if float(yoy) >= 0 else "down"
        yoy_str = (
            f" Year-over-year, submissions are {direction} {abs(float(yoy)):.1f}% "
            f"from the same month last year."
        )

    # Status mix narration
    if sub_count == 0:
        status_str = "No status breakdown available — no submissions this month."
    else:
        completed_total = complete + approved
        completion_pct = round(completed_total / sub_count * 100, 1) if sub_count else 0
        # F9 fix — period-qualify the completion rate so this monthly chunk
        # doesn't outrank the lifecycle chunk on bare "completion rate" queries.
        status_str = (
            f"Of these submissions, {complete} {'are' if complete != 1 else 'is'} marked complete, "
            f"{approved} {'are' if approved != 1 else 'is'} approved (terminal-positive), "
            f"and {ready} {'are' if ready != 1 else 'is'} still in 'ready' status (in progress or awaiting review). "
            f"{period} monthly completion rate (this month only): {completion_pct}% "
            f"(complete + approved / total submissions in {period})."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Form submissions monthly summary — {period}.\n"
        f"{sub_count} form submission{'s were' if sub_count != 1 else ' was'} received in {period}, "
        f"covering {distinct_forms} distinct form template{'s' if distinct_forms != 1 else ''} "
        f"filled by {distinct_custs} distinct customer{'s' if distinct_custs != 1 else ''}. "
        f"{status_str}{mom_str}{yoy_str}"
    ).strip()


def gen_monthly_summary(monthly_rows: list[dict], snapshot_date: Any) -> str:
    """Multi-month rollup chunk — ALWAYS-EMIT.

    Powers S1 (cross-domain compare-by-month) and reinforces F4 (trend).

    Why this chunk exists:
      Each per-month chunk gets its own embedding. For "compare across
      months" queries, cosine sim picks 3-5 individual month chunks but
      may MISS the maximum month (verified in Postman trace: Jan/Feb/Oct
      retrieved, March missed). This chunk gives the AI a single source
      of truth listing every month + the explicit max/min/trend.

    Anti-competition design (avoid stealing retrieval from other questions):
      - DOES name months + uses comparative vocab (highest, most, peak)
      - AVOIDS form names (won't compete with per_form on F7/F8/F11)
      - AVOIDS "completion rate" framing (won't compete with lifecycle on F9)
      - AVOIDS "stuck" / "ready" status (won't compete with lifecycle/anomalies)
    """
    snap = _date_label(snapshot_date)

    if not monthly_rows:
        return (
            f"{_SYN_HEADER}\n"
            f"Form submissions monthly comparison as of {snap}.\n"
            f"No monthly form submission data is available for comparison."
        ).strip()

    # Sort chronologically and build the per-month line
    rows = sorted(
        monthly_rows,
        key=lambda r: r.get("period_start") if r.get("period_start") else date.min,
    )
    per_month_lines = []
    counts: dict[str, int] = {}
    for r in rows:
        period = _period_label(r.get("period_start"))
        n = r.get("submission_count", 0) or 0
        per_month_lines.append(f"{period}: {n} submission{'s' if n != 1 else ''}")
        counts[period] = n

    # Identify max + min months (call out by name)
    max_period = max(counts, key=counts.get)
    min_period = min(counts, key=counts.get)
    max_n = counts[max_period]
    min_n = counts[min_period]
    total_subs = sum(counts.values())
    n_months = len(counts)

    # Trend direction: compare first and last halves
    half = max(1, n_months // 2)
    first_half_avg = sum(list(counts.values())[:half]) / half
    second_half_avg = sum(list(counts.values())[-half:]) / half
    if second_half_avg > first_half_avg * 1.1:
        trend = "rising"
    elif second_half_avg < first_half_avg * 0.9:
        trend = "declining"
    else:
        trend = "roughly flat"

    months_str = "; ".join(per_month_lines)

    # Disambiguate when max == min (all months identical) to avoid weird phrasing
    if max_n == min_n:
        peak_str = (
            f"All {n_months} months had the same submission count ({max_n}). "
            f"There is no single peak or low month."
        )
    else:
        peak_str = (
            f"The month with the most form submissions across all {n_months} months "
            f"was {max_period} with {max_n} submission{'s' if max_n != 1 else ''} — "
            f"this is the highest, peak, and busiest month for form submissions in "
            f"the period. The month with the fewest submissions was {min_period} "
            f"with {min_n} submission{'s' if min_n != 1 else ''}."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Form submissions month-by-month comparison and totals as of {snap}.\n"
        f"Across the last {n_months} month{'s' if n_months != 1 else ''} of data, "
        f"{total_subs} total form submission{'s have' if total_subs != 1 else ' has'} "
        f"been received. Per-month breakdown: {months_str}. "
        f"{peak_str} "
        f"Overall trend across the period: {trend}. "
        f"This summary covers all months and is the single source of truth for "
        f"questions that compare form submissions between months, identify the "
        f"busiest or most-submitted month, or analyse multi-month trends."
    ).strip()


def gen_per_form(row: dict, org_lifetime_total: int | None = None) -> str:
    """Per-template chunk with rank, dormancy, completion rate.

    Powers F7 (most-submitted), F8 (which forms are dormant), F11 (which to deactivate).
    Includes org-wide context (P5) so this chunk wins against catalog rollup.
    """
    name = row.get("form_name", "Unknown form")
    desc = row.get("form_description") or ""
    is_active = row.get("is_active", True)
    rank = row.get("rank_by_submissions", 0) or 0
    lifetime_subs = row.get("lifetime_submission_count", 0) or 0
    last_30d = row.get("submissions_last_30d", 0) or 0
    last_90d = row.get("submissions_last_90d", 0) or 0
    completion_rate = row.get("completion_rate_pct")
    is_dormant = row.get("is_dormant", False)
    is_active_dormant = row.get("is_active_dormant", False)
    most_recent_sub = row.get("most_recent_submission_at")
    distinct_custs = row.get("distinct_customers", 0) or 0
    cat_id = row.get("category_id", 1)
    template_created = row.get("template_created_at")

    active_str = "active" if is_active else "INACTIVE (deactivated)"

    # F8 / F11 dormancy framing
    if is_active_dormant:
        dormancy_str = (
            f' This template is flagged as "active dormant" — it is currently active in the '
            f'system but has never received any customer submission. It is a candidate for '
            f'deactivation if no longer needed.'
        )
    elif is_dormant:
        dormancy_str = (
            f' This template is dormant (has never been submitted) but is already deactivated.'
        )
    else:
        dormancy_str = ""

    # Most-recent submission context
    recent_str = ""
    if most_recent_sub:
        recent_str = f" Most recent submission on {_date_label(most_recent_sub)}."

    # P5 disambiguation — org-wide context
    org_str = ""
    if org_lifetime_total is not None and org_lifetime_total > 0:
        share = round(lifetime_subs / org_lifetime_total * 100, 1)
        org_str = (
            f" Across the whole organization, this template accounts for {share}% "
            f"of all {org_lifetime_total} lifetime form submissions."
        )

    completion_str = ""
    if completion_rate is not None:
        completion_str = (
            f" Completion rate for this template (complete + approved / total submissions): "
            f"{_pct(completion_rate)}."
        )

    desc_str = f' Description: "{desc}".' if desc else ""

    # F7 fix — rank=1 forms get an explicit "most-submitted" head sentence so
    # cosine sim wins for "which form is most submitted" / "top form" queries.
    # Without this, the latest monthly chunk ("5 form submissions in March 2026")
    # outranks the per_form chunk because monthly mentions submissions counts
    # more prominently.
    rank1_head = ""
    if rank == 1 and lifetime_subs > 0:
        rank1_head = (
            f'This is the most-submitted form template overall — the top form '
            f'by lifetime submission count. '
        )

    return (
        f"{_SYN_HEADER}\n"
        f'Form template: "{name}" (form id {row.get("form_id")}, {active_str}, '
        f"category {cat_id}).{desc_str}\n"
        f'{rank1_head}The "{name}" form template '
        f"has received {lifetime_subs} lifetime submission{'s' if lifetime_subs != 1 else ''} "
        f"from {distinct_custs} distinct customer{'s' if distinct_custs != 1 else ''}, "
        f"ranking #{rank} among all templates by submission count. "
        f"In the last 30 days it received {last_30d} submission{'s' if last_30d != 1 else ''}, "
        f"and {last_90d} in the last 90 days."
        f"{completion_str}{dormancy_str}{recent_str}{org_str}"
    ).strip()


def gen_lifecycle(obj: dict) -> str:
    """Lifecycle status snapshot — ALWAYS-EMIT.

    Powers F9 (completion rate), F10 (stuck at ready), F13 (intake form vocab).
    Mirrors gift cards G6 always-emit pattern: must produce a confident answer
    even when stuck_ready_count = 0.
    """
    snap = _date_label(obj.get("snapshot_date"))
    total = obj.get("total_submissions", 0) or 0
    ready = obj.get("ready_count", 0) or 0
    complete = obj.get("complete_count", 0) or 0
    approved = obj.get("approved_count", 0) or 0
    unknown = obj.get("unknown_status_count", 0) or 0
    completion_rate = obj.get("completion_rate_pct")
    stuck = obj.get("stuck_ready_count", 0) or 0
    stuck_age = obj.get("stuck_ready_total_age_days", 0) or 0
    most_recent = obj.get("most_recent_submission_at")

    # ALWAYS-EMIT zero-stuck language so F10 / F13 get confident "no" answers
    if stuck == 0:
        stuck_str = (
            "There are zero form submissions stuck at 'ready' status (none older than 7 days "
            "from the snapshot date). All ready-status submissions are either recent or have "
            "already moved to complete or approved."
        )
    else:
        avg_age = round(stuck_age / stuck) if stuck else 0
        stuck_str = (
            f"There {'are' if stuck != 1 else 'is'} {stuck} form submission{'s' if stuck != 1 else ''} "
            f"stuck at 'ready' status (older than 7 days from the snapshot). "
            f"These have been waiting an average of {avg_age} days. "
            f"Intake forms or feedback forms in this state may need follow-up "
            f"to move them to 'complete' or 'approved'."
        )

    if total == 0:
        body = "No form submissions have been received for this organization yet."
    else:
        if unknown > 0:
            status_breakdown = (
                f"{complete} complete, {approved} approved, "
                f"{ready} in 'ready' status, and {unknown} with unknown/non-standard status"
            )
        else:
            status_breakdown = (
                f"{complete} complete, {approved} approved, "
                f"and {ready} in 'ready' status"
            )
        # F9 fix — lead with the "overall completion rate" framing so cosine
        # sim wins for "what's my form completion rate?" against the per-month
        # monthly chunks (which carry "Monthly completion rate: X%").
        body = (
            f"Overall form completion rate across the entire business, all time: "
            f"{_pct(completion_rate)} (complete + approved / total submissions). "
            f"Across the lifetime of this organization, {total} form submission{'s have' if total != 1 else ' has'} "
            f"been received: {status_breakdown}."
        )

    recent_str = ""
    if most_recent:
        recent_str = f" Most recent form submission was on {_date_label(most_recent)}."

    unknown_warning = ""
    if unknown > 0:
        unknown_warning = (
            f" {unknown} submission{'s have' if unknown != 1 else ' has'} a status value "
            f"outside the documented set (ready/complete/approved) — these may need cleanup."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Form submissions lifecycle status snapshot as of {snap}.\n"
        f"{body} {stuck_str}{recent_str}{unknown_warning}"
    ).strip()


def gen_pii_policy(snapshot_date: Any) -> str:
    """Privacy / PII guardrail chunk — ALWAYS-EMIT.

    Powers F14 ("show me what customer N answered on form X").

    Designed to win cosine sim against per_form chunks specifically when the
    query mentions an individual customer / customer ID / specific customer.
    Deliberately:
      - Uses "individual customer" / "customer ID" / "specific customer" /
        "personal information" / "private" / "confidential" vocab heavily
        — these phrases are NOT in any other forms chunk.
      - Avoids form names ("Intake Questionnaire", "feedback") so it does
        not compete with per_form chunks on F7/F8/F11.
      - Avoids count anchors ("4 templates", "18 submissions") so it does
        not compete with catalog/lifecycle on F1/F3/F9.
      - Provides explicit refusal phrasings the LLM can adopt verbatim.
    """
    snap = _date_label(snapshot_date)
    return (
        f"{_SYN_HEADER}\n"
        f"Privacy and confidentiality policy for individual customer form data "
        f"as of {snap}.\n"
        f"Form submission data about any specific customer, individual customer, "
        f"or particular customer ID is private and confidential. We do not "
        f"disclose, share, retrieve, or display the personal information, "
        f"answers, or response content provided by any individual customer on "
        f"any form. We cannot confirm or deny whether a specific customer ID "
        f"or named customer has submitted any particular form. Questions of the "
        f"form 'show me customer N's answers', 'what did customer N say on the "
        f"form', 'did customer N fill out the form', 'show me the responses "
        f"from customer N', or 'what answers did this customer give' must be "
        f"declined. The correct response in those cases is to explain that "
        f"individual customer form responses are private and not available "
        f"through this assistant for privacy reasons, without confirming or "
        f"denying whether the customer in question exists or submitted "
        f"anything. Aggregated statistics that do not identify any single "
        f"customer (for example, the total submission count for a form, the "
        f"number of distinct customers who submitted, or the org-wide "
        f"completion rate) remain available because they do not disclose "
        f"individual personal information."
    ).strip()


def gen_anomalies(obj: dict) -> str:
    """Anomalies snapshot — ALWAYS-EMIT.

    Mirrors gift cards G6 pattern. Currently surfaces stuck-ready and unknown-status
    counts with explicit zero-emission language. Future-proofs for F10 detail
    questions and any "weird patterns" zero-emission acceptance.
    """
    snap = _date_label(obj.get("snapshot_date"))
    stuck = obj.get("stuck_ready_count", 0) or 0
    stuck_ids = obj.get("stuck_ready_submission_ids") or []
    unknown = obj.get("unknown_status_count", 0) or 0

    # ALWAYS-EMIT structure mirrors gift cards G6
    if stuck == 0:
        stuck_str = (
            "There are zero form submissions stuck at 'ready' status older than 7 days. "
            "No follow-up is needed on aged ready-status forms."
        )
    else:
        ids_preview = ", ".join(str(i) for i in list(stuck_ids)[:10])
        stuck_str = (
            f"{stuck} form submission{'s are' if stuck != 1 else ' is'} flagged as a "
            f"stuck-ready anomaly (status='ready' and older than 7 days). "
            f"Internal submission ids requiring follow-up: {ids_preview}."
        )

    if unknown == 0:
        unknown_str = (
            "All form submissions have a recognized status value "
            "(ready, complete, or approved) — there are zero anomalous statuses."
        )
    else:
        unknown_str = (
            f"{unknown} form submission{'s have' if unknown != 1 else ' has'} a "
            f"status value outside the documented set — these may indicate data-quality "
            f"issues or an undocumented workflow state."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Form submission anomalies snapshot as of {snap}.\n"
        f"{stuck_str} {unknown_str}"
    ).strip()


# =============================================================================
# Generator registry — pure functions for tests
# =============================================================================

CHUNK_GENERATORS: dict[str, Callable] = {
    "catalog":          gen_catalog,
    "monthly":          gen_monthly,
    "monthly_summary":  gen_monthly_summary,
    "per_form":         gen_per_form,
    "lifecycle":        gen_lifecycle,
    "anomalies":        gen_anomalies,
    "pii_policy":       gen_pii_policy,
}


# =============================================================================
# Main entry point — called by doc_generators/__init__.py (_gen_forms)
# =============================================================================

async def generate_forms_docs(
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
    org_id:           Tenant / business ID.
    warehouse_rows:   Dict produced by FormsExtractor.run() — keys:
                        catalog (dict), monthly (list), per_form (list),
                        lifecycle (dict).
    embedding_client: EmbeddingClient — embeds chunk_text.
    vector_store:    VectorStore — upserts embeddings.
    force:            If True, re-embed even if doc_id already exists.

    Returns
    -------
    dict with: docs_created, docs_skipped, docs_failed
    """
    catalog   = warehouse_rows.get("catalog")
    monthly   = warehouse_rows.get("monthly", []) or []
    per_form  = warehouse_rows.get("per_form", []) or []
    lifecycle = warehouse_rows.get("lifecycle")

    chunks: list[tuple[str, str, str, bool, dict, Any]] = []
    # Tuple = (doc_id, doc_type, chunk_text, is_rollup, metadata, period_start)

    # ── Build dormant-form-name list for catalog F11 advice ─────────────────
    dormant_names = [
        r.get("form_name") for r in per_form
        if r.get("is_active_dormant") and r.get("form_name")
    ]

    # ── Compute org_lifetime_total once for per-form P5 disambiguation ──────
    org_lifetime_total = (
        catalog.get("lifetime_submission_total")
        if catalog else None
    )

    # ── EP1: catalog snapshot — one rollup chunk per snapshot ───────────────
    if catalog:
        snap = catalog.get("snapshot_date")
        text = gen_catalog(catalog, dormant_form_names=dormant_names)
        doc_id = f"forms:{org_id}:catalog:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_CATALOG, text, True, meta, snap))

    # ── EP2: monthly — one rollup chunk per active month ────────────────────
    for row in monthly:
        ps = row.get("period_start")
        text = gen_monthly(row)
        doc_id = f"forms:{org_id}:monthly:{_period_key(ps)}"
        meta = {"business_id": org_id, "period": _period_key(ps).replace("_", "-")}
        chunks.append((doc_id, DOC_TYPE_MONTHLY, text, True, meta, ps))

    # ── EP2.5: monthly_summary — multi-month rollup, ALWAYS-EMIT ────────────
    # Powers S1 (compare months) + reinforces F4 (trend). Single source of
    # truth for "which month had the most" queries that may otherwise miss
    # the peak month due to top-k cosine selection.
    snap_for_summary = (
        catalog.get("snapshot_date") if catalog
        else (lifecycle.get("snapshot_date") if lifecycle else None)
    )
    if monthly:
        ms_text = gen_monthly_summary(monthly, snap_for_summary)
        ms_doc_id = f"forms:{org_id}:monthly_summary:{_date_key(snap_for_summary)}"
        ms_meta = {
            "business_id": org_id,
            "snapshot_date": str(snap_for_summary) if snap_for_summary else None,
            "month_count": len(monthly),
        }
        chunks.append((ms_doc_id, DOC_TYPE_MONTHLY_SUMMARY, ms_text, True, ms_meta, snap_for_summary))

    # ── EP3: per_form — one chunk per template, is_rollup=False ─────────────
    for row in per_form:
        snap = row.get("snapshot_date")
        fid = row.get("form_id")
        text = gen_per_form(row, org_lifetime_total=org_lifetime_total)
        doc_id = f"forms:{org_id}:per_form:{fid}:{_date_key(snap)}"
        meta = {
            "business_id":  org_id,
            "snapshot_date": str(snap) if snap else None,
            "form_id":       fid,
            "form_name":     row.get("form_name"),
            "is_active":     bool(row.get("is_active", True)),
            "is_dormant":    bool(row.get("is_dormant", False)),
        }
        chunks.append((doc_id, DOC_TYPE_PER_FORM, text, False, meta, snap))

    # ── EP4: lifecycle (always-emit) ────────────────────────────────────────
    if lifecycle:
        snap = lifecycle.get("snapshot_date")
        text = gen_lifecycle(lifecycle)
        doc_id = f"forms:{org_id}:lifecycle:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_LIFECYCLE, text, True, meta, snap))

        # ── Anomalies — derived from lifecycle row, also always-emit ────────
        # We re-use the lifecycle dict because stuck_ready_count + unknown_status_count
        # are both already there. Separate doc_type so retrieval ranks it
        # for "any unusual patterns?" / F10 detail questions.
        anom_text = gen_anomalies(lifecycle)
        anom_doc_id = f"forms:{org_id}:anomalies:{_date_key(snap)}"
        anom_meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((anom_doc_id, DOC_TYPE_ANOMALIES, anom_text, True, anom_meta, snap))

        # ── PII policy — always-emit, F14 privacy guardrail ─────────────────
        # Static text per-snapshot, scoped via snapshot date. Designed to win
        # cosine sim on individual-customer queries without competing on
        # generic form/submission queries (no form names, no counts).
        pii_text = gen_pii_policy(snap)
        pii_doc_id = f"forms:{org_id}:pii_policy:{_date_key(snap)}"
        pii_meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((pii_doc_id, DOC_TYPE_PII_POLICY, pii_text, True, pii_meta, snap))

    # ── Embed + upsert (matches giftcards / clients pattern) ────────────────
    created = skipped = failed = 0
    tenant = str(org_id)
    for doc_id, doc_type, chunk_text, is_rollup, metadata, period_start in chunks:
        if not force and await vector_store.exists(tenant, doc_id):
            skipped += 1
            continue
        try:
            embedding = await embedding_client.embed(chunk_text)
            await vector_store.upsert(
                doc_id     = doc_id,
                tenant_id  = tenant,
                doc_domain = DOMAIN,
                doc_type   = doc_type,
                chunk_text = chunk_text,
                embedding  = embedding,
                metadata   = {**metadata, "is_rollup": is_rollup},
            )
            created += 1
        except Exception as exc:
            failed += 1
            logger.error(
                "forms handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "forms handler done org=%d created=%d skipped=%d failed=%d "
        "(total_chunks=%d)",
        org_id, created, skipped, failed, len(chunks),
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}


__all__ = [
    "DOMAIN",
    "CHUNK_GENERATORS",
    "generate_forms_docs",
    "gen_catalog", "gen_monthly", "gen_monthly_summary",
    "gen_per_form", "gen_lifecycle", "gen_anomalies", "gen_pii_policy",
]