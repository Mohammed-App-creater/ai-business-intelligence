"""
queries/giftcards.py
====================
Gift card liability and usage queries (Production DB).

Tables used:
  tbl_giftcard — issued gift cards per org
  tbl_visit    — joined for redemption amounts (GCAmount, GCID)

Key columns (tbl_giftcard):
  id               — PK
  GiftCardNumber   — unique card identifier (varchar 50)
  GiftCardBalance  — current remaining balance (decimal 20,6)
  ActivationDate   — when card was activated
  OrgId            — tenant scoping
  Active           — 1 = active, 0 = inactive/depleted

Key columns (tbl_visit):
  GCID             — FK to tbl_giftcard.id (0 = no gift card used)
  GCAmount         — amount redeemed from gift card on this visit

Note: Individual gift card balance lookups ("is this card valid?")
go via the SaaS API. These DB queries handle analytics:
total liability, redemption trends, outstanding balances.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gift card liability summary
# ---------------------------------------------------------------------------

async def get_giftcard_liability(
    pool,
    org_id: int,
) -> dict[str, Any]:
    """
    Total outstanding gift card liability — all active cards with balance > 0.

    Returns a single dict like:
        {
          "active_cards": 45,
          "inactive_cards": 12,
          "total_cards": 57,
          "total_outstanding_balance": 2250.0,
          "avg_balance": 50.0
        }
    """
    sql = """
        SELECT
            SUM(CASE WHEN Active = 1 THEN 1 ELSE 0 END)      AS active_cards,
            SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END)      AS inactive_cards,
            COUNT(*)                                          AS total_cards,
            ROUND(
                SUM(CASE WHEN Active = 1
                    THEN GiftCardBalance ELSE 0 END), 2
            )                                                 AS total_outstanding_balance,
            ROUND(
                AVG(CASE WHEN Active = 1 AND GiftCardBalance > 0
                    THEN GiftCardBalance ELSE NULL END), 2
            )                                                 AS avg_balance
        FROM tbl_giftcard
        WHERE OrgId = %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            rows = await cur.fetchall()
            return rows[0] if rows else {}


# ---------------------------------------------------------------------------
# Gift cards issued per month
# ---------------------------------------------------------------------------

async def get_giftcards_issued_by_month(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    How many gift cards were issued (activated) per month.

    Returns rows like:
        {"month": "2026-03", "cards_issued": 12, "total_value": 600.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(ActivationDate, '%%Y-%%m')   AS month,
            COUNT(*)                                 AS cards_issued,
            ROUND(SUM(GiftCardBalance), 2)           AS total_value
        FROM tbl_giftcard
        WHERE OrgId          = %s
          AND ActivationDate BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(ActivationDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Gift card redemptions per month (from tbl_visit)
# ---------------------------------------------------------------------------

async def get_giftcard_redemptions(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Gift card redemptions per month from visit records.
    Uses tbl_visit.GCAmount and GCID (GCID > 0 means a gift card was used).

    Returns rows like:
        {
          "month": "2026-03",
          "redemption_count": 18,
          "total_redeemed": 720.0,
          "avg_redemption": 40.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(RecDateTime, '%%Y-%%m')     AS month,
            COUNT(*)                                AS redemption_count,
            ROUND(SUM(GCAmount), 2)                 AS total_redeemed,
            ROUND(AVG(GCAmount), 2)                 AS avg_redemption
        FROM tbl_visit
        WHERE OrganizationId = %s
          AND RecDateTime    BETWEEN %s AND %s
          AND PaymentStatus  = 1
          AND GCID           > 0
          AND GCAmount       > 0
        GROUP BY DATE_FORMAT(RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Gift cards with low or zero balance (nearly depleted)
# ---------------------------------------------------------------------------

async def get_low_balance_giftcards(
    pool,
    org_id: int,
    threshold: float = 10.0,
) -> list[dict[str, Any]]:
    """
    Active gift cards with balance at or below a threshold.
    Useful for identifying cards about to be fully redeemed.

    Returns rows like:
        {
          "id": 22,
          "gift_card_number": "GC-001234",
          "balance": 5.0,
          "activation_date": "2025-12-01"
        }
    """
    sql = """
        SELECT
            id                  AS id,
            GiftCardNumber      AS gift_card_number,
            ROUND(GiftCardBalance, 2) AS balance,
            ActivationDate      AS activation_date
        FROM tbl_giftcard
        WHERE OrgId           = %s
          AND Active          = 1
          AND GiftCardBalance <= %s
        ORDER BY GiftCardBalance ASC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, threshold))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Issued vs redeemed summary (liability vs cashflow)
# ---------------------------------------------------------------------------

async def get_giftcard_issued_vs_redeemed(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Month-by-month comparison of gift card value issued vs redeemed.
    Net liability = issued - redeemed.

    Returns rows like:
        {
          "month": "2026-03",
          "issued_value": 600.0,
          "redeemed_value": 300.0,
          "net_liability_change": 300.0
        }
    """
    sql = """
        SELECT
            months.month,
            ROUND(IFNULL(iss.issued_value,    0), 2)  AS issued_value,
            ROUND(IFNULL(red.redeemed_value,  0), 2)  AS redeemed_value,
            ROUND(
                IFNULL(iss.issued_value, 0) -
                IFNULL(red.redeemed_value, 0),
                2
            )                                         AS net_liability_change
        FROM (
            SELECT DATE_FORMAT(ActivationDate, '%%Y-%%m') AS month
            FROM tbl_giftcard
            WHERE OrgId = %s AND ActivationDate BETWEEN %s AND %s
            UNION
            SELECT DATE_FORMAT(RecDateTime, '%%Y-%%m') AS month
            FROM tbl_visit
            WHERE OrganizationId = %s AND RecDateTime BETWEEN %s AND %s
              AND PaymentStatus = 1 AND GCID > 0
        ) months
        LEFT JOIN (
            SELECT DATE_FORMAT(ActivationDate, '%%Y-%%m') AS month,
                   SUM(GiftCardBalance) AS issued_value
            FROM tbl_giftcard
            WHERE OrgId = %s AND ActivationDate BETWEEN %s AND %s
            GROUP BY DATE_FORMAT(ActivationDate, '%%Y-%%m')
        ) iss ON months.month = iss.month
        LEFT JOIN (
            SELECT DATE_FORMAT(RecDateTime, '%%Y-%%m') AS month,
                   SUM(GCAmount) AS redeemed_value
            FROM tbl_visit
            WHERE OrganizationId = %s AND RecDateTime BETWEEN %s AND %s
              AND PaymentStatus = 1 AND GCID > 0
            GROUP BY DATE_FORMAT(RecDateTime, '%%Y-%%m')
        ) red ON months.month = red.month
        ORDER BY months.month
    """
    params = (
        org_id, from_date, to_date,   # union - giftcard
        org_id, from_date, to_date,   # union - visit
        org_id, from_date, to_date,   # iss subquery
        org_id, from_date, to_date,   # red subquery
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()
