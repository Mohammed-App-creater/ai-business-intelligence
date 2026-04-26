"""
app/services/doc_generators/domains/giftcards.py
=================================================
Gift Cards domain chunk generator (Domain 9, Sprint 9).

Transforms warehouse rows (returned by GiftcardsExtractor.run()) into ~30-50
embedding documents per business. The doc gen reads from the in-memory dict
returned by the extractor, not from the warehouse — same pattern as clients.py.

Doc catalog (matches Step 2 + Step 3 EP1–EP8):
   1. monthly_summary        — per-period redemption + activation        Q1, Q4, Q5, Q7, Q21, Q27, Q29, S1, S2
   2. liability_snapshot     — outstanding balance summary               Q2, Q3, Q6, Q19, Q22
   3. by_staff               — per-(staff, period) — is_rollup=False     Q8
   4. by_location            — per-(location, period) — is_rollup=False  Q9, Q10, S3
   5. aging_bucket           — per-bucket rows (4 per snapshot)          Q14, Q26, Q28
   6. dormancy_summary       — single chunk per snapshot                 Q14, Q15
   7. anomalies_snapshot     — always-emit per Q31 acceptance            Q24, Q25, Q31
   8. denomination_snapshot  — combined 6-bucket chunk                   Q12
   9. health_snapshot        — lifetime population health                Q23, Q30

Lessons baked in
----------------
L4 — per-location chunks fragile against rollup.
     Per-location chunks have explicit "Branch: {name}" header so the
     retriever doesn't substitute the org-wide rollup chunk.
L5 — vocabulary tests catch routing gaps.
     Per-location chunks contain BOTH "branch" AND "location" tokens.
L6 — every chunk carries the synonym header so vocab variants
     (gift card / giftcard / prepaid card / gift voucher / stored value / GC)
     all retrieve.
P5 — rollup outranks per-location:
     Per-location chunks include the location name and the org-wide total
     for the same period to win on cosine sim against the rollup chunk.
P7 — PII refusal:
     No customer names, emails, or phone numbers in any chunk. Card numbers
     (GC-XXX strings) never appear; only internal integer card IDs in the
     anomalies chunk.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable

logger = logging.getLogger(__name__)

DOMAIN = "giftcards"

# Doc types — stable doc_id components
DOC_TYPE_MONTHLY      = "monthly_summary"
DOC_TYPE_LIABILITY    = "liability_snapshot"
DOC_TYPE_BY_STAFF     = "by_staff"
DOC_TYPE_BY_LOCATION  = "by_location"
DOC_TYPE_AGING        = "aging_bucket"
DOC_TYPE_DORMANCY     = "dormancy_summary"
DOC_TYPE_ANOMALIES    = "anomalies_snapshot"
DOC_TYPE_DENOMINATION = "denomination_snapshot"
DOC_TYPE_HEALTH       = "health_snapshot"


# =============================================================================
# Synonym header — appears at the top of EVERY chunk (Lesson 6)
# =============================================================================

_SYN_HEADER = (
    "Topic: gift card / giftcard / prepaid card / gift voucher / "
    "stored value / GC. Domain: gift cards."
)


# =============================================================================
# Formatters — pure helpers
# =============================================================================

def _money(v: Any) -> str:
    if v is None:
        return "$0.00"
    return f"${float(v):,.2f}"


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
    """For doc_ids — '2026_03' from a date or 'YYYY-MM-DD' string."""
    if d is None:
        return "unknown"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%Y_%m") if hasattr(d, "strftime") else str(d)


def _date_key(d: Any) -> str:
    """For doc_ids — '2026_03_31' from a date or 'YYYY-MM-DD' string."""
    if d is None:
        return "unknown"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%Y_%m_%d") if hasattr(d, "strftime") else str(d)


# =============================================================================
# Pure chunk text generators — no I/O, used by tests
# =============================================================================

def gen_monthly_summary(row: dict) -> str:
    period = _period_label(row.get("period_start"))
    red_count = row.get("redemption_count", 0) or 0
    red_total = row.get("redemption_amount_total", 0) or 0
    distinct = row.get("distinct_cards_redeemed", 0) or 0
    activations = row.get("activation_count", 0) or 0
    weekend = row.get("weekend_redemption_count", 0) or 0
    weekday = row.get("weekday_redemption_count", 0) or 0
    avg_uplift = row.get("avg_uplift_per_visit", 0) or 0
    uplift_total = row.get("uplift_total", 0) or 0
    mom_red = row.get("mom_redemption_pct")
    mom_act = row.get("mom_activation_pct")
    yoy_red = row.get("yoy_redemption_pct")

    mom_str = ""
    if mom_red is not None:
        direction = "up" if float(mom_red) >= 0 else "down"
        mom_str = f" Redemptions {direction} {abs(float(mom_red)):.1f}% versus the prior month."
    yoy_str = ""
    if yoy_red is not None:
        direction = "up" if float(yoy_red) >= 0 else "down"
        yoy_str = f" Year-over-year, redemptions are {direction} {abs(float(yoy_red)):.1f}% from the same month last year."

    if red_count == 0 and activations == 0:
        body = "No gift card activity recorded this month."
    elif red_count == 0:
        body = (
            f"No gift card redemptions this month, but {activations} "
            f"new gift card{'s' if activations != 1 else ''} "
            f"{'were' if activations != 1 else 'was'} activated (sold)."
        )
    else:
        body = (
            f"{red_count} gift card redemption{'s' if red_count != 1 else ''} "
            f"totaling {_money(red_total)}, "
            f"across {distinct} distinct gift card{'s' if distinct != 1 else ''}. "
            f"{weekday} weekday and {weekend} weekend visit{'s' if (weekday + weekend) != 1 else ''}. "
            f"Customers spent an additional {_money(uplift_total)} on top of "
            f"their gift cards (avg uplift {_money(avg_uplift)} per visit). "
            f"{activations} new gift card{'s' if activations != 1 else ''} "
            f"{'were' if activations != 1 else 'was'} activated this month."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card monthly summary — {period}.\n"
        f"{body}{mom_str}{yoy_str}"
    ).strip()


def gen_liability_snapshot(obj: dict) -> str:
    snap = _date_label(obj.get("snapshot_date"))
    active_n = obj.get("active_card_count", 0) or 0
    out_total = obj.get("outstanding_liability_total", 0) or 0
    avg_excl = obj.get("avg_remaining_balance_excl_drained", 0) or 0
    avg_incl = obj.get("avg_remaining_balance_incl_drained", 0) or 0
    drained = obj.get("drained_active_count", 0) or 0
    median = obj.get("median_remaining_balance", 0) or 0

    # NOTE on vocab choices (Step 7 fix for Q20):
    # The phrasing below is engineered to win retrieval on questions like
    # "How much stored value do customers still have?" and "What's the total
    # outstanding gift card balance?" — so this chunk outranks the aging
    # bucket chunks (which would otherwise sum to a partial total).
    return (
        f"{_SYN_HEADER}\n"
        f"Outstanding gift card liability snapshot as of {snap}.\n"
        f"Customers still have a total of {_money(out_total)} in stored value "
        f"across {active_n} active gift cards — this is the total unredeemed "
        f"balance customers can still use. The total outstanding gift card "
        f"liability is {_money(out_total)}, meaning the business owes customers "
        f"this amount in unused gift card balances. "
        f"Average remaining balance per card: {_money(avg_excl)} excluding fully "
        f"drained, {_money(avg_incl)} including drained. "
        f"Median remaining balance: {_money(median)}. "
        f"{drained} active gift card{'s are' if drained != 1 else ' is'} flagged as anomalies "
        f"with zero remaining balance (drained but still marked active)."
    ).strip()


def gen_by_staff(row: dict) -> str:
    period = _period_label(row.get("period_start"))
    name = row.get("staff_name", "Unknown")
    is_active = row.get("is_active", 1)
    count = row.get("redemption_count", 0) or 0
    total = row.get("redemption_amount_total", 0) or 0
    distinct = row.get("distinct_cards_redeemed", 0) or 0
    rank = row.get("rank_in_period", 0) or 0
    active_str = "" if is_active else " (no longer active staff member)"

    return (
        f"{_SYN_HEADER}\n"
        f"Staff: {name}{active_str}. Gift card redemption activity for {period}.\n"
        f"{name} processed {count} gift card redemption{'s' if count != 1 else ''} "
        f"totaling {_money(total)} in {period}, "
        f"covering {distinct} distinct gift card{'s' if distinct != 1 else ''}. "
        f"This ranks #{rank} among all staff who processed gift card redemptions in {period}."
    ).strip()


def gen_by_location(row: dict, org_total: float | None = None) -> str:
    """Per-location chunk — has both 'branch' AND 'location' (L5),
    includes org-wide context for disambiguation (P5)."""
    period = _period_label(row.get("period_start"))
    loc_name = row.get("location_name", "Unknown")
    count = row.get("redemption_count", 0) or 0
    total = row.get("redemption_amount_total", 0) or 0
    distinct = row.get("distinct_cards_redeemed", 0) or 0
    pct_org = row.get("pct_of_org_redemption")
    mom = row.get("mom_redemption_pct")

    pct_str = ""
    if pct_org is not None:
        pct_str = f" ({_pct(pct_org)} of business-wide gift card redemption for {period})"
    org_str = ""
    if org_total is not None:
        org_str = f" The org-wide total for {period} was {_money(org_total)}."
    mom_str = ""
    if mom is not None:
        direction = "up" if float(mom) >= 0 else "down"
        mom_str = f" This branch is {direction} {abs(float(mom)):.1f}% versus the prior month."

    return (
        f"{_SYN_HEADER}\n"
        f"Branch: {loc_name} — gift card redemption activity by location for {period}.\n"
        f"The {loc_name} branch (location id {row.get('location_id')}) processed "
        f"{count} gift card redemption{'s' if count != 1 else ''} totaling "
        f"{_money(total)}{pct_str}, covering {distinct} distinct "
        f"gift card{'s' if distinct != 1 else ''}.{org_str}{mom_str}"
    ).strip()


def gen_aging_bucket(row: dict) -> str:
    snap = _date_label(row.get("snapshot_date"))
    bucket = row.get("age_bucket", "?")
    count = row.get("card_count", 0) or 0
    liab = row.get("liability_amount", 0) or 0
    pct = row.get("pct_of_total_liability")
    never = row.get("never_redeemed_in_bucket", 0) or 0

    bucket_label = {
        "0-30":   "0 to 30 days old (newest gift cards)",
        "31-90":  "31 to 90 days old (about one to three months)",
        "91-180": "91 to 180 days old (three to six months)",
        "181+":   "older than 180 days (more than six months)",
    }.get(bucket, bucket)

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card aging bucket {bucket} as of {snap}.\n"
        f"{count} active gift card{'s are' if count != 1 else ' is'} {bucket_label}. "
        f"These cards hold {_money(liab)} in unused balance, representing {_pct(pct)} of "
        f"total outstanding liability. {never} card{'s in this bucket have' if never != 1 else ' in this bucket has'} "
        f"never been redeemed (purchased but sitting unused)."
    ).strip()


def gen_dormancy_summary(row: dict) -> str:
    snap = _date_label(row.get("snapshot_date"))
    count = row.get("card_count", 0) or 0
    liab = row.get("liability_amount", 0) or 0
    avg_dtf = row.get("avg_days_to_first_redemption")
    longest_id = row.get("longest_dormant_card_id")
    longest_days = row.get("longest_dormant_days")

    avg_str = ""
    if avg_dtf is not None:
        avg_str = (
            f" Across active gift cards that have been redeemed at least once, "
            f"it takes an average of {float(avg_dtf):.0f} days from activation to first redemption."
        )
    longest_str = ""
    if longest_id is not None and longest_days is not None:
        longest_str = (
            f" The longest-dormant card has been sitting unused for {longest_days} days "
            f"(internal card id {longest_id})."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card dormancy summary as of {snap}.\n"
        f"You have {count} never-redeemed active gift card{'s' if count != 1 else ''} "
        f"holding {_money(liab)} in unused balance — these were sold but never used by customers."
        f"{longest_str}{avg_str}"
    ).strip()


def gen_anomalies_snapshot(obj: dict) -> str:
    """Always-emit anomalies chunk. Q31 acceptance criterion."""
    snap = _date_label(obj.get("snapshot_date"))
    drained_n = obj.get("drained_active_count", 0) or 0
    drained_ids = obj.get("drained_active_card_ids") or []
    deact_n = obj.get("deactivated_count", 0) or 0
    deact_value = obj.get("deactivated_value_total_derived", 0) or 0
    refund_n = obj.get("refunded_redemption_count", 0) or 0
    refund_amt = obj.get("refunded_redemption_amount", 0) or 0
    p_start = _date_label(obj.get("period_start"))
    p_end = _date_label(obj.get("period_end"))

    drained_str = (
        f"{drained_n} gift card{'s are' if drained_n != 1 else ' is'} flagged as a drained-active anomaly "
        f"(zero balance but still marked active in the system)"
    )
    if drained_ids:
        ids_preview = ", ".join(str(i) for i in list(drained_ids)[:10])
        drained_str += f" — internal card ids: {ids_preview}"
    drained_str += "."

    deact_str = (
        f"{deact_n} gift card{'s have' if deact_n != 1 else ' has'} been deactivated, "
        f"with a derived original face value totaling {_money(deact_value)}."
    )

    if refund_n == 0:
        refund_str = (
            f"There were zero refunded gift card redemptions in the period "
            f"{p_start} through {p_end}."
        )
    else:
        refund_str = (
            f"There were {refund_n} refunded gift card redemption{'s' if refund_n != 1 else ''} "
            f"totaling {_money(refund_amt)} in the period {p_start} through {p_end}."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card anomalies snapshot as of {snap}.\n"
        f"{drained_str} {deact_str} {refund_str}"
    ).strip()


def gen_denomination_snapshot(rows: list[dict]) -> str:
    if not rows:
        return f"{_SYN_HEADER}\nNo gift card denomination data available."
    snap = _date_label(rows[0].get("snapshot_date"))
    total_cards = sum(int(r.get("card_count", 0) or 0) for r in rows)
    total_value = sum(float(r.get("total_value_issued", 0) or 0) for r in rows)

    nonzero = [r for r in rows if int(r.get("card_count", 0) or 0) > 0]
    top = max(nonzero, key=lambda r: int(r.get("card_count", 0))) if nonzero else None

    bucket_lines = []
    for r in rows:
        n = int(r.get("card_count", 0) or 0)
        if n == 0:
            continue
        bucket = r.get("denomination_bucket", "?")
        val = float(r.get("total_value_issued", 0) or 0)
        avg = float(r.get("avg_face_value", 0) or 0)
        pct = float(r.get("pct_of_cards", 0) or 0)
        bucket_lines.append(
            f"{bucket}: {n} card{'s' if n != 1 else ''} "
            f"({_money(val)} total, avg {_money(avg)}, {pct:.1f}% of cards)"
        )

    top_str = ""
    if top is not None:
        top_n = int(top["card_count"])
        top_str = (
            f" The most common denomination is {top['denomination_bucket']} "
            f"with {top_n} card{'s' if top_n != 1 else ''} "
            f"({float(top['pct_of_cards']):.1f}% of all gift cards issued)."
        )

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card denomination distribution as of {snap}.\n"
        f"Across all {total_cards} gift cards ever issued (face value total {_money(total_value)}), "
        f"the breakdown by face value bucket is:\n"
        + "\n".join("  - " + ln for ln in bucket_lines)
        + top_str
    ).strip()


def gen_health_snapshot(obj: dict) -> str:
    snap = _date_label(obj.get("snapshot_date"))
    total = obj.get("total_cards_issued", 0) or 0
    redeemed = obj.get("cards_with_redemption", 0) or 0
    rate = obj.get("redemption_rate_pct")
    single = obj.get("single_visit_drained_count", 0) or 0
    multi = obj.get("multi_visit_redeemed_count", 0) or 0
    single_pct = obj.get("single_visit_drained_pct_of_redeemed")
    multi_pct = obj.get("multi_visit_redeemed_pct_of_redeemed")
    distinct = obj.get("distinct_customer_redeemers", 0) or 0

    return (
        f"{_SYN_HEADER}\n"
        f"Gift card population health as of {snap}.\n"
        f"{redeemed} of {total} gift cards ever issued have been redeemed at least once, "
        f"giving a lifetime redemption rate of {_pct(rate)}. "
        f"Of the redeemed cards, {single} ({_pct(single_pct)}) were used in a single visit, "
        f"while {multi} ({_pct(multi_pct)}) took multiple visits to redeem. "
        f"{distinct} distinct customer{'s have' if distinct != 1 else ' has'} ever redeemed a gift card."
    ).strip()


# =============================================================================
# Generator registry — pure functions for tests
# =============================================================================

CHUNK_GENERATORS: dict[str, Callable] = {
    "monthly_summary":       gen_monthly_summary,
    "liability_snapshot":    gen_liability_snapshot,
    "by_staff":              gen_by_staff,
    "by_location":           gen_by_location,
    "aging_bucket":          gen_aging_bucket,
    "dormancy_summary":      gen_dormancy_summary,
    "anomalies_snapshot":    gen_anomalies_snapshot,
    "denomination_snapshot": gen_denomination_snapshot,
    "health_snapshot":       gen_health_snapshot,
}


# =============================================================================
# Main entry point — called by doc_generators/__init__.py (_gen_giftcards)
# =============================================================================

async def generate_giftcards_docs(
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
    warehouse_rows:   Dict produced by GiftcardsExtractor.run() — keys:
                        monthly, liability, by_staff, by_location,
                        aging, anomalies, denomination, health.
    embedding_client: EmbeddingClient — embeds chunk_text.
    vector_store:    VectorStore — upserts embeddings.
    force:            If True, re-embed even if doc_id already exists.

    Returns
    -------
    dict with: docs_created, docs_skipped, docs_failed
    """
    monthly      = warehouse_rows.get("monthly", []) or []
    liability    = warehouse_rows.get("liability")
    by_staff     = warehouse_rows.get("by_staff", []) or []
    by_location  = warehouse_rows.get("by_location", []) or []
    aging        = warehouse_rows.get("aging", []) or []
    anomalies    = warehouse_rows.get("anomalies")
    denomination = warehouse_rows.get("denomination", []) or []
    health       = warehouse_rows.get("health")

    chunks: list[tuple[str, str, str, bool, dict, Any]] = []
    # Tuple = (doc_id, doc_type, chunk_text, is_rollup, metadata, period_start)

    # ── EP1: monthly — one rollup chunk per month ───────────────────────────
    for row in monthly:
        ps = row.get("period_start")
        text = gen_monthly_summary(row)
        doc_id = f"giftcards:{org_id}:monthly:{_period_key(ps)}"
        meta = {"business_id": org_id, "period": _period_key(ps).replace("_", "-")}
        chunks.append((doc_id, DOC_TYPE_MONTHLY, text, True, meta, ps))

    # ── EP2: liability snapshot — one rollup chunk per snapshot ────────────
    if liability:
        snap = liability.get("snapshot_date")
        text = gen_liability_snapshot(liability)
        doc_id = f"giftcards:{org_id}:liability:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_LIABILITY, text, True, meta, snap))

    # ── EP3: by_staff — one chunk per (staff, period), is_rollup=False ─────
    for row in by_staff:
        ps = row.get("period_start")
        sid = row.get("staff_id")
        text = gen_by_staff(row)
        doc_id = f"giftcards:{org_id}:staff:{sid}:{_period_key(ps)}"
        meta = {
            "business_id": org_id,
            "period": _period_key(ps).replace("_", "-"),
            "staff_id": sid,
        }
        chunks.append((doc_id, DOC_TYPE_BY_STAFF, text, False, meta, ps))

    # ── EP4: by_location — one chunk per (location, period), is_rollup=False
    # P5 disambiguation: include org-wide period total for context
    org_total_by_period = {
        r.get("period_start"): float(r.get("redemption_amount_total", 0) or 0)
        for r in monthly
    }
    for row in by_location:
        ps = row.get("period_start")
        lid = row.get("location_id")
        text = gen_by_location(row, org_total=org_total_by_period.get(ps))
        doc_id = f"giftcards:{org_id}:location:{lid}:{_period_key(ps)}"
        meta = {
            "business_id": org_id,
            "period": _period_key(ps).replace("_", "-"),
            "location_id": lid,
            "location_name": row.get("location_name"),
        }
        chunks.append((doc_id, DOC_TYPE_BY_LOCATION, text, False, meta, ps))

    # ── EP5: aging — 4 bucket rows + 1 dormancy_summary row ────────────────
    for row in aging:
        snap = row.get("snapshot_date")
        if row.get("row_type") == "aging_bucket":
            text = gen_aging_bucket(row)
            doc_id = f"giftcards:{org_id}:aging:{row.get('age_bucket')}:{_date_key(snap)}"
            meta = {
                "business_id": org_id,
                "snapshot_date": str(snap) if snap else None,
                "age_bucket": row.get("age_bucket"),
            }
            chunks.append((doc_id, DOC_TYPE_AGING, text, True, meta, snap))
        else:
            text = gen_dormancy_summary(row)
            doc_id = f"giftcards:{org_id}:dormancy:{_date_key(snap)}"
            meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
            chunks.append((doc_id, DOC_TYPE_DORMANCY, text, True, meta, snap))

    # ── EP6: anomalies (always-emit) ───────────────────────────────────────
    if anomalies:
        snap = anomalies.get("snapshot_date")
        text = gen_anomalies_snapshot(anomalies)
        doc_id = f"giftcards:{org_id}:anomalies:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_ANOMALIES, text, True, meta, snap))

    # ── EP7: denomination distribution — one combined chunk per snapshot ───
    if denomination:
        snap = denomination[0].get("snapshot_date")
        text = gen_denomination_snapshot(denomination)
        doc_id = f"giftcards:{org_id}:denomination:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_DENOMINATION, text, True, meta, snap))

    # ── EP8: health snapshot ────────────────────────────────────────────────
    if health:
        snap = health.get("snapshot_date")
        text = gen_health_snapshot(health)
        doc_id = f"giftcards:{org_id}:health:{_date_key(snap)}"
        meta = {"business_id": org_id, "snapshot_date": str(snap) if snap else None}
        chunks.append((doc_id, DOC_TYPE_HEALTH, text, True, meta, snap))

    # ── Embed + upsert (matches clients.py pattern) ─────────────────────────
    created = skipped = failed = 0
    for doc_id, doc_type, chunk_text, is_rollup, metadata, period_start in chunks:
        if not force and await vector_store.exists(doc_id):
            skipped += 1
            continue
        try:
            embedding = await embedding_client.embed(chunk_text)
            await vector_store.upsert(
                doc_id     = doc_id,
                tenant_id  = str(org_id),
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
                "giftcards handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "giftcards handler done org=%d created=%d skipped=%d failed=%d "
        "(total_chunks=%d)",
        org_id, created, skipped, failed, len(chunks),
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}


__all__ = [
    "DOMAIN",
    "CHUNK_GENERATORS",
    "generate_giftcards_docs",
    "gen_monthly_summary", "gen_liability_snapshot", "gen_by_staff",
    "gen_by_location", "gen_aging_bucket", "gen_dormancy_summary",
    "gen_anomalies_snapshot", "gen_denomination_snapshot", "gen_health_snapshot",
]