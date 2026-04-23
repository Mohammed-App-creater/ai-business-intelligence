"""
app/services/doc_generators/domains/promos.py
==============================================
Promos domain chunk generator.

Transforms warehouse rows (from wh_promo_monthly, wh_promo_codes,
wh_promo_locations, wh_promo_location_codes, wh_promo_catalog_health)
into ~30-50 embedding documents per business depending on code/location count.

Doc catalog (matches Step 2 query spec — 6 chunk types):
  1. promo_monthly_summary       — Q1, Q2, Q4-Q8, Q12, Q26 (per-period rollup)
  2. promo_code_monthly          — Q9, Q11, Q13-Q15, Q24, Q25 (per-code per-period)
  3. promo_code_window_total     — Q3, Q10 (per-code window aggregate)
  4. promo_location_monthly      — Q18-Q21 (per-loc per-code per-period)
  5. promo_location_rollup       — Q18-Q21 (per-loc per-period rollup)
  6. promo_catalog_health        — Q22, Q23 (catalog state, period=NULL)

Vocabulary engineering:
  Each chunk embeds the synonym set "promo, promo code, coupon, discount,
  redemption, redeemed, offer, deal" so vector retrieval wins for
  vocabulary-variant questions (per Lesson 6 from prior sprints).

Lesson 5 (branch+location both present): per-location chunks use BOTH
words "branch" AND "location" so location-name retrieval wins on either term.

Lesson 7 (cross-location comparison gap): per-location chunks set
location_id to the actual location, NOT zero, so vector_store's
exclude_rollup filter can isolate them when the user asks "compare branches".
Rollup chunks (org-wide / code-aggregate) use location_id=0.

NULL-safe rendering: orphan promo IDs (FK orphan, see Step 2 N1) arrive
with promo_code_string=NULL and promo_label=NULL. We render them as
"unknown promo (ID #N)" so chunks remain readable.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

DOMAIN = "promos"

# Doc types — used as stable doc_id components
DOC_TYPE_MONTHLY_SUMMARY    = "promo_monthly_summary"
DOC_TYPE_CODE_MONTHLY       = "promo_code_monthly"
DOC_TYPE_CODE_WINDOW        = "promo_code_window_total"
DOC_TYPE_LOCATION_MONTHLY   = "promo_location_monthly"
DOC_TYPE_LOCATION_ROLLUP    = "promo_location_rollup"
DOC_TYPE_CATALOG_HEALTH     = "promo_catalog_health"


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point — called by DocGenerator._gen_promos
# ─────────────────────────────────────────────────────────────────────────────

async def generate_promo_docs(
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
    warehouse_rows:   Dict produced by PromosExtractor.run() with keys:
                        monthly, codes_monthly, codes_window,
                        locations_rollup, locations_by_code, catalog_health
    embedding_client: EmbeddingClient — embeds chunk_text.
    vector_store:    VectorStore — upserts embeddings.
    force:           If True, re-embed even if doc_id already exists.

    Returns
    -------
    dict with: docs_created, docs_skipped, docs_failed
    """
    monthly       = warehouse_rows.get("monthly") or []
    codes_m       = warehouse_rows.get("codes_monthly") or []
    codes_w       = warehouse_rows.get("codes_window") or []
    locs_rollup   = warehouse_rows.get("locations_rollup") or []
    locs_by_code  = warehouse_rows.get("locations_by_code") or []
    catalog       = warehouse_rows.get("catalog_health") or []

    if not any([monthly, codes_m, codes_w, locs_rollup, locs_by_code, catalog]):
        logger.info("promos handler: no warehouse data for org=%d — skipping", org_id)
        return {"docs_created": 0, "docs_skipped": 0, "docs_failed": 0}

    chunks: list[tuple[str, str, str, bool, dict]] = []
    # Tuple = (doc_id, doc_type, chunk_text, is_rollup, metadata)

    chunks.extend(_build_monthly_chunks(org_id, monthly))
    chunks.extend(_build_code_monthly_chunks(org_id, codes_m))
    chunks.extend(_build_code_window_chunks(org_id, codes_w))
    chunks.extend(_build_location_monthly_chunks(org_id, locs_by_code))
    chunks.extend(_build_location_rollup_chunks(org_id, locs_rollup))
    chunks.extend(_build_catalog_health_chunks(org_id, catalog))

    # ── Embed + upsert ────────────────────────────────────────────────────────
    created = skipped = failed = 0
    tenant = str(org_id)

    for doc_id, doc_type, chunk_text, is_rollup, metadata in chunks:
        if not force and await vector_store.exists(tenant, doc_id):
            skipped += 1
            continue

        try:
            embedding = await embedding_client.embed(chunk_text)
            # Pull the date out — json.dumps in vector_store can't serialise it
            period_start = metadata.get("period_start_date")
            clean_meta = {k: v for k, v in metadata.items() if k != "period_start_date"}
            clean_meta["is_rollup"] = is_rollup
            await vector_store.upsert(
                tenant_id=tenant,
                doc_id=doc_id,
                doc_domain=DOMAIN,
                doc_type=doc_type,
                chunk_text=chunk_text,
                embedding=embedding,
                period_start=period_start,
                metadata=clean_meta,
            )
            created += 1
        except Exception as exc:
            failed += 1
            logger.error(
                "promos handler: failed doc_id=%s doc_type=%s org=%d error=%r",
                doc_id, doc_type, org_id, exc,
            )

    logger.info(
        "promos handler done org=%d created=%d skipped=%d failed=%d "
        "(total_chunks=%d)",
        org_id, created, skipped, failed, len(chunks),
    )
    return {"docs_created": created, "docs_skipped": skipped, "docs_failed": failed}


# ─────────────────────────────────────────────────────────────────────────────
# CHUNK BUILDERS — one function per doc type
# Each returns a list of (doc_id, doc_type, chunk_text, is_rollup, metadata)
# ─────────────────────────────────────────────────────────────────────────────

def _build_monthly_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 1 — promo_monthly_summary.
    One chunk per period. Powers Q1, Q2, Q4-Q8, Q12, Q26.
    Vocabulary: promo, coupon, discount, redemption, redeemed, offer."""
    out = []
    for r in rows:
        period_str = _period_str(r.get("period_start"))
        period_lbl = _period_label(period_str)
        period_dt = _parse_date(r.get("period_start"))

        red = int(r.get("promo_redemptions") or 0)
        disc = float(r.get("total_discount_given") or 0)
        codes_used = int(r.get("distinct_codes_used") or 0)
        visit_pct = r.get("promo_visit_pct")
        avg_disc = r.get("avg_discount_per_redemption")
        prev_red = r.get("prev_month_redemptions")
        prev_disc = r.get("prev_month_discount")

        # MoM narrative
        if prev_red is not None and prev_red > 0:
            mom_red_pct = (red - prev_red) / prev_red * 100
            mom_red_txt = (
                f"Compared to the prior month ({int(prev_red)} redemptions), "
                f"this is a {mom_red_pct:+.1f}% change."
            )
        else:
            mom_red_txt = "No prior-month redemption data available for comparison."

        if prev_disc is not None and prev_disc > 0:
            mom_disc_pct = (disc - prev_disc) / prev_disc * 100
            mom_disc_txt = (
                f"Total discount given changed by {mom_disc_pct:+.1f}% "
                f"versus prior month (${prev_disc:,.2f})."
            )
        else:
            mom_disc_txt = ""

        visit_pct_txt = f"{visit_pct:.2f}%" if visit_pct is not None else "N/A"
        avg_disc_txt = f"${float(avg_disc):.2f}" if avg_disc is not None else "N/A"

        text = (
            f"Promo summary for {period_lbl} (also called coupon, discount, "
            f"redemption, or offer activity). "
            f"Total promo redemptions this month: {red}. "
            f"Total discount given through promos: ${disc:,.2f}. "
            f"Distinct promo codes used: {codes_used}. "
            f"Promo visit percentage (paid visits using a promo code): {visit_pct_txt}. "
            f"Average discount per redemption: {avg_disc_txt}. "
            f"{mom_red_txt} {mom_disc_txt}"
        ).strip()

        doc_id = f"promos:{org_id}:monthly:{period_str}"
        meta = {
            "period": period_str,
            "period_start_date": period_dt,
            "business_id": org_id,
            "location_id": 0,  # org-wide rollup
            "redemptions": red,
            "total_discount": disc,
        }
        # is_rollup=True — org-wide period rollup
        out.append((doc_id, DOC_TYPE_MONTHLY_SUMMARY, text, True, meta))
    return out


def _build_code_monthly_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 2 — promo_code_monthly.
    One chunk per (period × code). Powers Q9, Q11, Q13-Q15, Q24, Q25.
    Vocabulary: promo code, coupon code, code activity."""
    out = []
    for r in rows:
        period_str = _period_str(r.get("period_start"))
        period_lbl = _period_label(period_str)
        period_dt = _parse_date(r.get("period_start"))

        promo_id = int(r["promo_id"])
        code = _code_label(r.get("promo_code_string"), promo_id)
        label = r.get("promo_label")
        amount_meta = r.get("promo_amount_metadata")

        red = int(r.get("redemptions") or 0)
        disc = float(r.get("total_discount") or 0)
        avg_d = r.get("avg_discount")
        max_d = r.get("max_single_discount")

        # Render label part — be honest about NULLs
        if label:
            code_intro = f"Promo code {code} (labeled '{label}')"
        else:
            code_intro = f"Promo code {code}"

        # Show metadata Amount but DO NOT confuse it with actual discount
        if amount_meta is not None:
            meta_note = (
                f" The catalog metadata Amount field for this code is "
                f"${float(amount_meta):.2f} — note this is metadata, not a "
                f"per-redemption discount value."
            )
        else:
            meta_note = ""

        avg_d_txt = f"${float(avg_d):.2f}" if avg_d is not None else "N/A"
        max_d_txt = f"${float(max_d):.2f}" if max_d is not None else "N/A"

        text = (
            f"{code_intro} performance in {period_lbl}. "
            f"Times redeemed (coupon usage, discount applications): {red}. "
            f"Total discount given through this code: ${disc:,.2f}. "
            f"Average discount per redemption: {avg_d_txt}. "
            f"Largest single-visit discount: {max_d_txt}."
            f"{meta_note}"
        ).strip()

        doc_id = f"promos:{org_id}:code:{promo_id}:{period_str}"
        meta = {
            "period": period_str,
            "period_start_date": period_dt,
            "business_id": org_id,
            "location_id": 0,  # code-aggregate is org-wide
            "promo_id": promo_id,
            "promo_code_string": r.get("promo_code_string"),
            "redemptions": red,
            "total_discount": disc,
        }
        out.append((doc_id, DOC_TYPE_CODE_MONTHLY, text, True, meta))
    return out


def _build_code_window_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 3 — promo_code_window_total.
    One chunk per code (window aggregate, no period). Powers Q3, Q10.
    Vocabulary: most-redeemed, top promo, total uses, biggest discount."""
    out = []
    # Sort by total_discount desc so retrieval naturally favors top performers
    sorted_rows = sorted(
        rows, key=lambda r: -(float(r.get("total_discount") or 0))
    )

    for rank, r in enumerate(sorted_rows, start=1):
        promo_id = int(r["promo_id"])
        code = _code_label(r.get("promo_code_string"), promo_id)
        label = r.get("promo_label")

        red = int(r.get("redemptions") or 0)
        disc = float(r.get("total_discount") or 0)
        avg_d = r.get("avg_discount")
        max_d = r.get("max_single_discount")
        is_active = r.get("is_active")
        exp = r.get("expiration_date")
        is_expired_now = r.get("is_expired_now")

        # Status narrative
        status_parts = []
        if is_active is not None:
            status_parts.append("active" if is_active else "inactive")
        if exp:
            status_parts.append(f"expires {_date_str(exp)}")
        if is_expired_now == 1 and is_active == 1:
            status_parts.append("ALREADY EXPIRED but flagged active in catalog")
        status_txt = ", ".join(status_parts) if status_parts else "status unknown"

        if label:
            code_intro = f"Promo code {code} (labeled '{label}')"
        else:
            code_intro = f"Promo code {code}"

        avg_d_txt = f"${float(avg_d):.2f}" if avg_d is not None else "N/A"
        max_d_txt = f"${float(max_d):.2f}" if max_d is not None else "N/A"

        text = (
            f"{code_intro} — full-window aggregate (across all reporting periods). "
            f"Status: {status_txt}. "
            f"Total times redeemed in window: {red}. "
            f"Total discount given through this code: ${disc:,.2f}. "
            f"Average discount per redemption: {avg_d_txt}. "
            f"Largest single redemption: {max_d_txt}. "
            f"Discount-volume rank in window: #{rank} of {len(sorted_rows)} codes."
        )

        doc_id = f"promos:{org_id}:code_window:{promo_id}"
        meta = {
            "business_id": org_id,
            "location_id": 0,
            "promo_id": promo_id,
            "promo_code_string": r.get("promo_code_string"),
            "rank": rank,
            "redemptions_window": red,
            "total_discount_window": disc,
            "period_start_date": None,  # window-total — no period
        }
        out.append((doc_id, DOC_TYPE_CODE_WINDOW, text, True, meta))
    return out


def _build_location_monthly_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 4 — promo_location_monthly.
    One chunk per (period × location × code). Powers Q18-Q21.
    is_rollup=False — these are per-location and excludable via exclude_rollup."""
    out = []
    for r in rows:
        period_str = _period_str(r.get("period_start"))
        period_lbl = _period_label(period_str)
        period_dt = _parse_date(r.get("period_start"))

        loc_id = int(r["location_id"])
        loc_name = r.get("location_name") or f"Location {loc_id}"
        promo_id = int(r["promo_id"])
        code = _code_label(r.get("promo_code_string"), promo_id)
        label = r.get("promo_label")

        red = int(r.get("redemptions") or 0)
        disc = float(r.get("total_discount") or 0)
        avg_d = r.get("avg_discount")

        if label:
            code_intro = f"Promo code {code} (labeled '{label}')"
        else:
            code_intro = f"Promo code {code}"

        avg_d_txt = f"${float(avg_d):.2f}" if avg_d is not None else "N/A"

        # Lesson 5 — say BOTH "branch" AND "location" plus the literal name
        text = (
            f"{code_intro} activity at the {loc_name} location (also called "
            f"a branch or site) for {period_lbl}. "
            f"Times this code was redeemed at {loc_name}: {red}. "
            f"Total discount given at {loc_name} via this code: ${disc:,.2f}. "
            f"Average discount per redemption at {loc_name}: {avg_d_txt}."
        )

        doc_id = f"promos:{org_id}:loc_code:{loc_id}:{promo_id}:{period_str}"
        meta = {
            "period": period_str,
            "period_start_date": period_dt,
            "business_id": org_id,
            "location_id": loc_id,           # ← actual location, NOT 0
            "location_name": loc_name,
            "promo_id": promo_id,
            "promo_code_string": r.get("promo_code_string"),
            "redemptions": red,
            "total_discount": disc,
        }
        # is_rollup=False — per-location chunk
        out.append((doc_id, DOC_TYPE_LOCATION_MONTHLY, text, False, meta))
    return out


def _build_location_rollup_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 5 — promo_location_rollup.
    One chunk per (period × location), aggregating across all codes.
    Powers Q18-Q21. is_rollup=False — per-location, NOT org-wide."""
    out = []
    for r in rows:
        period_str = _period_str(r.get("period_start"))
        period_lbl = _period_label(period_str)
        period_dt = _parse_date(r.get("period_start"))

        loc_id = int(r["location_id"])
        loc_name = r.get("location_name") or f"Location {loc_id}"

        red = int(r.get("total_promo_redemptions") or 0)
        codes_used = int(r.get("distinct_codes_used") or 0)
        disc = float(r.get("total_discount_given") or 0)
        avg_d = r.get("avg_discount_per_redemption")

        avg_d_txt = f"${float(avg_d):.2f}" if avg_d is not None else "N/A"

        # Lesson 5 — say BOTH "branch" AND "location" plus the literal name
        text = (
            f"Total promo activity at the {loc_name} location (also called "
            f"a branch or site) for {period_lbl}, aggregated across all "
            f"promo codes (coupons, discounts, offers). "
            f"Total promo redemptions at {loc_name}: {red}. "
            f"Distinct promo codes used at {loc_name}: {codes_used}. "
            f"Total discount given to customers at {loc_name}: ${disc:,.2f}. "
            f"Average discount per redemption at {loc_name}: {avg_d_txt}."
        )

        doc_id = f"promos:{org_id}:loc_rollup:{loc_id}:{period_str}"
        meta = {
            "period": period_str,
            "period_start_date": period_dt,
            "business_id": org_id,
            "location_id": loc_id,           # ← actual location, NOT 0
            "location_name": loc_name,
            "redemptions": red,
            "total_discount": disc,
        }
        # is_rollup=False — per-location chunk (the name "rollup" here means
        # "rolled up across codes", not "org-wide rollup")
        out.append((doc_id, DOC_TYPE_LOCATION_ROLLUP, text, False, meta))
    return out


def _build_catalog_health_chunks(org_id: int, rows: list[dict]) -> list[tuple]:
    """Doc type 6 — promo_catalog_health.
    One chunk per code (catalog snapshot, no period). Powers Q22, Q23.
    Vocabulary: dormant, expired, active-but-expired, unused, stale."""
    out = []
    for r in rows:
        promo_id = int(r["promo_id"])
        code = _code_label(r.get("promo_code_string"), promo_id)
        label = r.get("promo_label")
        is_active = r.get("is_active")
        exp = r.get("expiration_date")
        is_expired = r.get("is_expired") == 1
        active_but_expired = r.get("active_but_expired") == 1
        red_90d = int(r.get("redemptions_last_90d") or 0)
        is_dormant = r.get("is_dormant") == 1
        snap = _date_str(r.get("snapshot_date"))

        # Status narrative
        status_parts = []
        if is_active == 1 and not is_expired:
            status_parts.append("active and within validity period")
        elif is_active == 1 and is_expired:
            status_parts.append(
                "marked active in catalog BUT past expiration date "
                "(active-but-expired data quality flag)"
            )
        elif is_active == 0:
            status_parts.append("inactive (deactivated in catalog)")
        else:
            status_parts.append("status unknown")

        if exp:
            status_parts.append(f"expiration date: {_date_str(exp)}")

        if is_dormant:
            status_parts.append(
                f"DORMANT — zero redemptions in last 90 days "
                f"(stale, unused code)"
            )

        if label:
            code_intro = f"Promo code {code} (labeled '{label}')"
        else:
            code_intro = f"Promo code {code}"

        text = (
            f"{code_intro} catalog health snapshot as of {snap}. "
            f"{'. '.join(status_parts)}. "
            f"Redemptions in last 90 days: {red_90d}."
        )

        doc_id = f"promos:{org_id}:catalog:{promo_id}"
        meta = {
            "business_id": org_id,
            "location_id": 0,
            "promo_id": promo_id,
            "promo_code_string": r.get("promo_code_string"),
            "is_active": is_active,
            "is_expired": is_expired,
            "active_but_expired": active_but_expired,
            "is_dormant": is_dormant,
            "period_start_date": None,  # catalog-style — no period
        }
        out.append((doc_id, DOC_TYPE_CATALOG_HEALTH, text, True, meta))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _code_label(code: Optional[str], promo_id: int) -> str:
    """Render orphan codes (NULL promo_code_string) as 'unknown promo (ID #N)'."""
    if code:
        return f"'{code}'"
    return f"'unknown promo (ID #{promo_id})'"


def _period_str(value) -> str:
    """Normalize period_start to a YYYY-MM-DD string for use in doc_id and metadata."""
    if value is None:
        return "null"
    s = str(value)[:10]
    return s


def _period_label(period_str: str) -> str:
    """'2026-03-01' → 'March 2026'. Falls back to the raw string."""
    from datetime import date
    try:
        d = date.fromisoformat(period_str[:10])
        return d.strftime("%B %Y")
    except (ValueError, TypeError):
        return period_str


def _date_str(value) -> str:
    """Normalize date-like values to a 'YYYY-MM-DD' string. None → 'unknown'."""
    if value is None:
        return "unknown"
    return str(value)[:10]


def _parse_date(v):
    """Accept date, string, or None; return date or None."""
    from datetime import date
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None