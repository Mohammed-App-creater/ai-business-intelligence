"""
queries/promos.py
=================
Promo code analytics queries (Production DB).

Tables used:
  tbl_promo  — platform-level promo codes (NO OrgId — LEO platform scope)
  tbl_visit  — visit records joined for per-org promo usage analytics

IMPORTANT SCOPE NOTE:
  tbl_promo has NO OrgId. These are LEO platform promo codes applied
  to SaaS subscriptions (e.g. "SUMMER20 gives 20% off for 3 months").

  For visit-level promo usage per org, we join tbl_visit.PromoCode (int FK)
  to tbl_promo.Id and scope by tbl_visit.OrganizationId.

tbl_promo columns:
  Id, PromoCode (varchar 20), PromoExpiration (date), Active (tinyint, default 0),
  Desc (varchar 150), Amount (decimal 20,6), SubscriptionCycle (tinyint)

tbl_visit.PromoCode — int FK → tbl_promo.Id (NULL = no promo used)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Promo catalog
# ---------------------------------------------------------------------------

async def get_promo_catalog(
    pool,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """
    All platform promo codes — used to look up what a promo offers.
    No org filter — these are platform-level records.

    Returns rows like:
        {
          "id": 3,
          "promo_code": "SUMMER20",
          "description": "Summer discount",
          "amount": 20.0,
          "expiration_date": "2026-08-31",
          "subscription_cycle": 3,
          "active": 1
        }
    """
    active_clause = "WHERE Active = 1" if active_only else ""
    sql = f"""
        SELECT
            Id                AS id,
            PromoCode         AS promo_code,
            `Desc`            AS description,
            Amount            AS amount,
            PromoExpiration   AS expiration_date,
            SubscriptionCycle AS subscription_cycle,
            Active            AS active
        FROM tbl_promo
        {active_clause}
        ORDER BY PromoExpiration DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, ())
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Active promos (not expired)
# ---------------------------------------------------------------------------

async def get_active_promos(
    pool,
) -> list[dict[str, Any]]:
    """
    Promos that are active AND not yet expired.
    Used for LOOKUP: "what promo codes are valid right now?"

    Returns rows like:
        {
          "id": 3,
          "promo_code": "SUMMER20",
          "description": "Summer discount",
          "amount": 20.0,
          "expiration_date": "2026-08-31",
          "subscription_cycle": 3
        }
    """
    sql = """
        SELECT
            Id                AS id,
            PromoCode         AS promo_code,
            `Desc`            AS description,
            Amount            AS amount,
            PromoExpiration   AS expiration_date,
            SubscriptionCycle AS subscription_cycle
        FROM tbl_promo
        WHERE Active          = 1
          AND PromoExpiration >= CURDATE()
        ORDER BY PromoExpiration ASC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, ())
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Promo usage per org (visit-level, org-scoped)
# ---------------------------------------------------------------------------

async def get_promo_usage_by_org(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    How many times each promo was used in client visits for an org,
    enriched with promo metadata. Joins tbl_visit → tbl_promo.

    Returns rows like:
        {
          "promo_id": 3,
          "promo_code": "SUMMER20",
          "description": "Summer discount",
          "times_used": 14,
          "total_discount": 280.0,
          "avg_discount_per_visit": 20.0
        }
    """
    sql = """
        SELECT
            p.Id                          AS promo_id,
            p.PromoCode                   AS promo_code,
            p.`Desc`                      AS description,
            COUNT(v.ID)                   AS times_used,
            ROUND(SUM(v.Discount),  2)    AS total_discount,
            ROUND(AVG(v.Discount),  2)    AS avg_discount_per_visit
        FROM tbl_visit v
        JOIN tbl_promo p ON v.PromoCode = p.Id
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
          AND v.PromoCode      IS NOT NULL
        GROUP BY p.Id, p.PromoCode, p.`Desc`
        ORDER BY times_used DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Monthly promo impact per org
# ---------------------------------------------------------------------------

async def get_monthly_promo_impact(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Month-by-month promo usage and total discount given for an org.

    Returns rows like:
        {
          "month": "2026-03",
          "promo_visits": 18,
          "total_discount_given": 360.0,
          "pct_visits_with_promo": 12.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(v.RecDateTime, '%%Y-%%m')           AS month,
            SUM(CASE WHEN v.PromoCode IS NOT NULL
                THEN 1 ELSE 0 END)                          AS promo_visits,
            ROUND(
                SUM(CASE WHEN v.PromoCode IS NOT NULL
                    THEN v.Discount ELSE 0 END), 2
            )                                               AS total_discount_given,
            ROUND(
                SUM(CASE WHEN v.PromoCode IS NOT NULL
                    THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0),
                1
            )                                               AS pct_visits_with_promo
        FROM tbl_visit v
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY DATE_FORMAT(v.RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()
