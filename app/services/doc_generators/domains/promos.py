"""
app/services/doc_generators/domains/promos.py
==============================================
Promos domain document handler for DocGenerator (Domain 8).

Reads the 5 warehouse tables produced by PromosExtractor and generates
human-readable chunk_text for each row, then stores embeddings in pgvector.

Called by DocGenerator.generate_all() when domain="promos" or domain=None.

Document types handled (6):
    promo_monthly_summary       → one doc per period (location_id=0 rollup)
    promo_code_monthly          → one doc per (period, code)
    promo_code_window_total     → one doc per code (period_start=NULL)
    promo_location_monthly      → one doc per (period, location, code)
    promo_location_rollup       → one doc per (period, location)
    promo_catalog_health        → one doc per code (period_start=NULL)

CRITICAL VOCABULARY DESIGN (Lessons 4, 5, 6 from prior sprints):

  Lesson 4 — Rollup docs outrank per-location docs for "by location"
  questions. Mitigation: rollup chunks have `is_rollup=true` metadata,
  retriever filters them out when _LOCATION_COMPARE_PHRASES match.

  Lesson 5 — Vocab mismatch hurts cosine similarity. Every location chunk
  header MUST include both "branch" AND "location" (not one or the other).

  Lesson 6 — Synonym pre-bake. Every promo chunk includes the synonym set
  early in the text: "promo / promo code / coupon / discount / offer".
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "promos"

PROMOS_DOC_TYPES = {
    "promo_monthly_summary",
    "promo_code_monthly",
    "promo_code_window_total",
    "promo_location_monthly",
    "promo_location_rollup",
    "promo_catalog_health",
}

# Synonym header — appears at the top of EVERY promo chunk so cosine similarity
# matches regardless of which word the user asks with.
_PROMO_SYNONYMS = (
    "promo / promo code / coupon / discount / offer / deal / redemption / savings"
)


# ─────────────────────────────────────────────────────────────────────────────
# Chunk text generators
# Each takes a warehouse row dict + business context, returns chunk_text str.
# ─────────────────────────────────────────────────────────────────────────────

def _format_promo_label(row: dict) -> str:
    """Render a code's display label safely. Handles orphans + NULL Desc."""
    code_string = row.get("promo_code_string")
    label       = row.get("promo_label")

    if code_string is None:
        # Orphan — promo_id has no matching tbl_promo row
        return f"Unknown code (id={row['promo_id']}, missing from your promo catalog)"

    if not label or label.strip() == "":
        return f"Code {code_string} (no label)"

    return f"Code {code_string} (labeled \"{label}\")"


def chunk_text_monthly_summary(row: dict, business_name: str = "your business") -> str:
    """
    Org-rollup monthly chunk — Q1, Q2, Q4–Q8, Q12, Q26.
    Carries is_rollup=true in metadata to allow exclusion when retriever
    detects per-location intent (Lesson 4).
    """
    period   = row["period_start"]
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)

    pct       = row["promo_visit_pct"]
    redemp    = row["promo_redemptions"]
    visits    = row["total_visits"]
    discount  = row["total_discount_given"]
    distinct  = row["distinct_codes_used"]
    avg_d     = row.get("avg_discount_per_redemption")

    # Build the body
    parts = [
        f"Promo redemption summary for {business_name} — {period_str}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Period: {period_str}. Scope: rollup across all branches and all locations.",
        "",
        f"In {period_str}, {redemp} of {visits} visits used a promo code "
        f"({pct}% of all visits). Total discount given through promos: ${discount}.",
    ]

    if avg_d is not None:
        parts.append(f"Average discount per redemption: ${avg_d}.")

    parts.append(f"Distinct promo codes redeemed this month: {distinct}.")

    # MoM context
    prev_red = row.get("prev_month_redemptions")
    prev_dis = row.get("prev_month_discount")
    if prev_red is not None:
        delta = redemp - prev_red
        direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
        parts.append(
            f"Month-over-month: redemptions {direction} from {prev_red} to {redemp} "
            f"(change of {delta:+d}). Previous month total discount: ${prev_dis}."
        )

    return "\n".join(parts)


def chunk_text_code_monthly(row: dict, business_name: str = "your business") -> str:
    """
    Per (period, code) chunk — Q14, Q15.
    These are the "how is THIS code performing in THIS month" chunks.
    """
    period = row["period_start"]
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)

    label = _format_promo_label(row)
    redemp = row["redemptions"]
    discount = row["total_discount"]
    avg_d = row.get("avg_discount")
    max_d = row.get("max_single_discount")

    parts = [
        f"Promo code performance for {business_name} — {period_str}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Period: {period_str}. {label}.",
        "",
        f"In {period_str}, {label} was redeemed {redemp} times, "
        f"totaling ${discount} in discount given.",
    ]

    if avg_d is not None:
        parts.append(f"Average discount per redemption: ${avg_d}.")
    if max_d is not None:
        parts.append(f"Largest single discount via this code: ${max_d}.")

    # Catalog state context
    if row.get("expiration_date"):
        parts.append(f"Code expiration date: {row['expiration_date']}.")
    if row.get("is_active") is not None:
        active_str = "active" if row["is_active"] == 1 else "inactive"
        parts.append(f"Code status: {active_str}.")

    return "\n".join(parts)


def chunk_text_code_window(row: dict, business_name: str = "your business",
                            window_label: str = "the last 6 months") -> str:
    """
    Per-code over the full window — Q3, Q9 (window), Q10, Q11, Q24.
    period_start IS NULL on these (catalog-style).
    """
    label = _format_promo_label(row)
    redemp = row["total_redemptions"]
    discount = row["total_discount"]
    avg_d = row.get("avg_discount")
    max_d = row.get("max_single_discount")

    parts = [
        f"Promo code lifetime performance for {business_name} — {window_label}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Window: {window_label}. {label}.",
        "",
        f"Across {window_label}, {label} was redeemed {redemp} times in total, "
        f"giving ${discount} in cumulative discount.",
    ]

    if avg_d is not None:
        parts.append(f"Average discount per redemption: ${avg_d}.")
    if max_d is not None:
        parts.append(f"Largest single discount on a visit using this code: ${max_d}.")

    # Status flags
    if row.get("is_expired_now") == 1:
        parts.append("Status note: this code's expiration date has passed.")
    if row.get("is_active") == 1 and row.get("is_expired_now") == 1:
        parts.append("Data quality flag: code is marked active but expired — "
                     "consider deactivating or extending the expiration date.")

    return "\n".join(parts)


def chunk_text_location_rollup(row: dict, business_name: str = "your business") -> str:
    """
    Per (period, location) rollup — Q18, Q19, Q21.
    Lesson 5: header includes both 'branch' AND 'location'.
    """
    period = row["period_start"]
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)
    loc_name = row.get("location_name") or f"Location {row['location_id']}"

    redemp = row["total_promo_redemptions"]
    distinct = row["distinct_codes_used"]
    discount = row["total_discount_given"]
    avg_d = row.get("avg_discount_per_redemption")

    parts = [
        f"Promo redemption per branch / per location — {business_name}, {period_str}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Branch: {loc_name}. Location: {loc_name}. Period: {period_str}.",
        "",
        f"At branch {loc_name} (location {loc_name}) in {period_str}, "
        f"{redemp} promo redemptions occurred, "
        f"using {distinct} distinct promo code(s). "
        f"Total discount given at this branch: ${discount}.",
    ]

    if avg_d is not None:
        parts.append(f"Average discount per redemption at {loc_name}: ${avg_d}.")

    return "\n".join(parts)


def chunk_text_location_monthly(row: dict, business_name: str = "your business") -> str:
    """
    Per (period, location, code) — Q20 detailed cross.
    """
    period = row["period_start"]
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)
    loc_name = row.get("location_name") or f"Location {row['location_id']}"
    label = _format_promo_label(row)

    redemp = row["redemptions"]
    discount = row["total_discount"]
    avg_d = row.get("avg_discount")

    parts = [
        f"Promo code performance by branch / location — {business_name}, {period_str}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Branch: {loc_name}. Location: {loc_name}. Period: {period_str}. {label}.",
        "",
        f"At branch {loc_name} in {period_str}, {label} was redeemed "
        f"{redemp} times, giving ${discount} in discount.",
    ]

    if avg_d is not None:
        parts.append(f"Average discount per redemption: ${avg_d}.")

    return "\n".join(parts)


def chunk_text_catalog_health(row: dict, business_name: str = "your business") -> str:
    """
    Catalog state snapshot — Q22, Q23.
    period_start IS NULL — catalog-style.
    """
    label = _format_promo_label(row)
    snap = row.get("snapshot_date")
    snap_str = snap.strftime("%Y-%m-%d") if hasattr(snap, "strftime") else str(snap)

    parts = [
        f"Promo code catalog health — {business_name}.",
        f"Topic: {_PROMO_SYNONYMS}.",
        f"Catalog snapshot date: {snap_str}. {label}.",
        "",
    ]

    # Status
    if row.get("is_active") == 1:
        parts.append(f"Status: marked active in your promo catalog.")
    elif row.get("is_active") == 0:
        parts.append(f"Status: marked inactive.")

    if row.get("expiration_date"):
        parts.append(f"Expiration date: {row['expiration_date']}.")

    # Health flags
    flags = []
    if row.get("active_but_expired") == 1:
        flags.append(
            f"⚠ DATA QUALITY: {label} is marked ACTIVE but its expiration "
            f"date has passed. Customers cannot redeem it. Consider "
            f"deactivating it or extending the expiration."
        )
    if row.get("is_dormant") == 1:
        flags.append(
            f"⚠ DORMANT: {label} is active but has had ZERO redemptions in "
            f"the last 90 days. Consider retiring it or running a campaign "
            f"to revive it."
        )

    if flags:
        parts.extend(flags)
    else:
        recent = row.get("redemptions_last_90d", 0)
        parts.append(
            f"Recent activity: {recent} redemptions in the last 90 days. "
            f"This code is healthy."
        )

    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Generator function — called by DocGenerator
# ─────────────────────────────────────────────────────────────────────────────

async def generate(
    self,           # DocGenerator instance
    business_id: int,
    business_name: str = "your business",
) -> tuple[int, int, int]:
    """
    Generate and embed all promo chunks for one tenant.

    Returns (created, skipped, failed) counts — same shape as other domain
    generators (revenue, marketing, etc.).

    Called by DocGenerator.generate_all() when domain="promos" or domain=None.
    """
    created = skipped = failed = 0

    # Read all 5 warehouse tables
    monthly       = await self._wh.promos.monthly(business_id)
    codes_monthly = await self._wh.promos.codes_monthly(business_id)
    codes_window  = await self._wh.promos.codes_window(business_id)
    loc_rollup    = await self._wh.promos.locations_rollup(business_id)
    loc_codes     = await self._wh.promos.location_codes(business_id)
    catalog       = await self._wh.promos.catalog_health(business_id)

    if not any([monthly, codes_monthly, codes_window, loc_rollup, loc_codes, catalog]):
        logger.info(
            "promos.doc_gen no_data business_id=%s — tenant has no promo activity",
            business_id,
        )
        return (0, 0, 0)

    # 1. Monthly rollup chunks (Lesson 4 — flag with is_rollup=true)
    for row in monthly:
        try:
            chunk = chunk_text_monthly_summary(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_monthly_summary",
                period_start   = row["period_start"],
                location_id    = 0,                    # 0 = rollup convention
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":  True,                # Lesson 4 — exclude_rollup hook
                    "promo_id":   None,
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=monthly_summary period=%s err=%r",
                         row["period_start"], exc)
            failed += 1

    # 2. Per-code monthly chunks
    for row in codes_monthly:
        try:
            chunk = chunk_text_code_monthly(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_code_monthly",
                period_start   = row["period_start"],
                location_id    = 0,                    # not location-scoped
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":  False,
                    "promo_id":   row["promo_id"],
                    "is_orphan":  row.get("promo_code_string") is None,
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=code_monthly promo_id=%s err=%r",
                         row.get("promo_id"), exc)
            failed += 1

    # 3. Per-code window-total chunks (period_start IS NULL — Lesson 3)
    for row in codes_window:
        try:
            chunk = chunk_text_code_window(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_code_window_total",
                period_start   = None,                 # catalog-style — Lesson 3
                location_id    = 0,
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":  False,
                    "promo_id":   row["promo_id"],
                    "is_orphan":  row.get("promo_code_string") is None,
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=code_window promo_id=%s err=%r",
                         row.get("promo_id"), exc)
            failed += 1

    # 4. Per-location rollup chunks
    for row in loc_rollup:
        try:
            chunk = chunk_text_location_rollup(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_location_rollup",
                period_start   = row["period_start"],
                location_id    = row["location_id"],
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":     False,             # NOT a rollup despite name —
                                                        # it's a per-location chunk
                    "location_id":   row["location_id"],
                    "location_name": row.get("location_name"),
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=location_rollup loc=%s period=%s err=%r",
                         row.get("location_id"), row["period_start"], exc)
            failed += 1

    # 5. Per-location per-code detail chunks
    for row in loc_codes:
        try:
            chunk = chunk_text_location_monthly(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_location_monthly",
                period_start   = row["period_start"],
                location_id    = row["location_id"],
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":     False,
                    "location_id":   row["location_id"],
                    "location_name": row.get("location_name"),
                    "promo_id":      row["promo_id"],
                    "is_orphan":     row.get("promo_code_string") is None,
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=location_monthly loc=%s code=%s err=%r",
                         row.get("location_id"), row.get("promo_id"), exc)
            failed += 1

    # 6. Catalog health chunks (period_start IS NULL — Lesson 3)
    for row in catalog:
        try:
            chunk = chunk_text_catalog_health(row, business_name)
            await self._embed_and_store(
                business_id    = business_id,
                doc_domain     = DOMAIN,
                doc_type       = "promo_catalog_health",
                period_start   = None,                 # catalog-style
                location_id    = 0,
                chunk_text     = chunk,
                metadata       = {
                    "is_rollup":          False,
                    "promo_id":           row["promo_id"],
                    "is_active":          row.get("is_active"),
                    "is_dormant":         row.get("is_dormant"),
                    "active_but_expired": row.get("active_but_expired"),
                },
            )
            created += 1
        except Exception as exc:
            logger.error("promos.chunk_failed type=catalog_health promo_id=%s err=%r",
                         row.get("promo_id"), exc)
            failed += 1

    logger.info(
        "promos.doc_gen complete business_id=%s created=%d skipped=%d failed=%d",
        business_id, created, skipped, failed,
    )

    return (created, skipped, failed)