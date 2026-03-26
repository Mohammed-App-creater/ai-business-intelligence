"""
queries/revenue.py
==================
Revenue queries against tbl_visit (Production DB).

All queries:
  - Require org_id — tenant isolation enforced at function signature level
  - Filter PaymentStatus = 1 (Success only) — never include failed/refunded visits
  - Accept from_date / to_date as datetime objects or ISO strings
  - Return list[dict] — plain Python, no ORM

Key tbl_visit columns used:
  TotalPay     — gross revenue (service + tips + tax - discounts)
  Payment      — service amount only
  Tips         — tip amount
  Discount     — discount applied
  GCAmount     — gift card redemption amount
  Tax          — tax collected
  PaymentType  — 'Cash' | 'Card' | 'Check' | etc.
  PromoCode    — int FK to tbl_promo.Id (NULL if no promo)
  EmpID        — staff who performed the service
  CustID       — customer
  RecDateTime  — when the visit was recorded
  PaymentStatus — 1=Success, 0=Failed, 2=Pending, 3=Revoked, 4=Refunded, 5=Canceled
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Monthly revenue summary
# ---------------------------------------------------------------------------

async def get_monthly_revenue(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Revenue aggregated by month and payment type.

    Returns rows like:
        {
          "month": "2026-03",
          "payment_type": "Card",
          "visit_count": 142,
          "total_revenue": 12400.0,
          "service_revenue": 11200.0,
          "tips": 800.0,
          "discounts": 200.0,
          "gift_card_redeemed": 150.0,
          "tax": 180.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(RecDateTime, '%%Y-%%m')  AS month,
            PaymentType                          AS payment_type,
            COUNT(*)                             AS visit_count,
            ROUND(SUM(TotalPay),  2)             AS total_revenue,
            ROUND(SUM(Payment),   2)             AS service_revenue,
            ROUND(SUM(Tips),      2)             AS tips,
            ROUND(SUM(Discount),  2)             AS discounts,
            ROUND(SUM(GCAmount),  2)             AS gift_card_redeemed,
            ROUND(SUM(IFNULL(Tax, 0)), 2)        AS tax
        FROM tbl_visit
        WHERE OrganizationId  = %s
          AND RecDateTime     BETWEEN %s AND %s
          AND PaymentStatus   = 1
        GROUP BY
            DATE_FORMAT(RecDateTime, '%%Y-%%m'),
            PaymentType
        ORDER BY month, payment_type
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Monthly revenue totals (collapsed — no payment type split)
# ---------------------------------------------------------------------------

async def get_monthly_revenue_totals(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Single row per month — all payment types combined.
    Best for trend charts and MoM comparisons.

    Returns rows like:
        {
          "month": "2026-03",
          "visit_count": 150,
          "total_revenue": 9200.0,
          "service_revenue": 8100.0,
          "tips": 650.0,
          "discounts": 300.0,
          "gift_card_redeemed": 200.0,
          "tax": 140.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(RecDateTime, '%%Y-%%m')  AS month,
            COUNT(*)                             AS visit_count,
            ROUND(SUM(TotalPay),  2)             AS total_revenue,
            ROUND(SUM(Payment),   2)             AS service_revenue,
            ROUND(SUM(Tips),      2)             AS tips,
            ROUND(SUM(Discount),  2)             AS discounts,
            ROUND(SUM(GCAmount),  2)             AS gift_card_redeemed,
            ROUND(SUM(IFNULL(Tax, 0)), 2)        AS tax
        FROM tbl_visit
        WHERE OrganizationId  = %s
          AND RecDateTime     BETWEEN %s AND %s
          AND PaymentStatus   = 1
        GROUP BY DATE_FORMAT(RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Revenue by payment type (period total)
# ---------------------------------------------------------------------------

async def get_revenue_by_payment_type(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Cash vs Card vs Gift Card breakdown for a period.

    Returns rows like:
        {"payment_type": "Card", "visit_count": 98, "total": 7800.0}
    """
    sql = """
        SELECT
            PaymentType              AS payment_type,
            COUNT(*)                 AS visit_count,
            ROUND(SUM(TotalPay), 2)  AS total
        FROM tbl_visit
        WHERE OrganizationId  = %s
          AND RecDateTime     BETWEEN %s AND %s
          AND PaymentStatus   = 1
        GROUP BY PaymentType
        ORDER BY total DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Revenue by staff member
# ---------------------------------------------------------------------------

async def get_revenue_by_staff(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Revenue and visit count per staff member for a period.
    Joins tbl_emp for name — only active employees included.

    Returns rows like:
        {
          "emp_id": 12,
          "first_name": "Maria",
          "last_name": "Garcia",
          "visit_count": 87,
          "total_revenue": 5200.0,
          "avg_ticket": 59.77,
          "tips": 420.0
        }
    """
    sql = """
        SELECT
            e.id                              AS emp_id,
            e.FirstName                       AS first_name,
            e.LastName                        AS last_name,
            COUNT(v.ID)                       AS visit_count,
            ROUND(SUM(v.TotalPay),  2)        AS total_revenue,
            ROUND(AVG(v.TotalPay),  2)        AS avg_ticket,
            ROUND(SUM(v.Tips),      2)        AS tips
        FROM tbl_visit v
        JOIN tbl_emp e ON v.EmpID = e.id
        WHERE v.OrganizationId  = %s
          AND v.RecDateTime     BETWEEN %s AND %s
          AND v.PaymentStatus   = 1
          AND e.Active          = 1
        GROUP BY e.id, e.FirstName, e.LastName
        ORDER BY total_revenue DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Daily revenue (for trend / spike detection)
# ---------------------------------------------------------------------------

async def get_daily_revenue(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Revenue per day — useful for detecting spikes and dips.

    Returns rows like:
        {"date": "2026-03-15", "visit_count": 12, "total_revenue": 980.0}
    """
    sql = """
        SELECT
            DATE(RecDateTime)        AS date,
            COUNT(*)                 AS visit_count,
            ROUND(SUM(TotalPay), 2)  AS total_revenue
        FROM tbl_visit
        WHERE OrganizationId  = %s
          AND RecDateTime     BETWEEN %s AND %s
          AND PaymentStatus   = 1
        GROUP BY DATE(RecDateTime)
        ORDER BY date
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Promo code usage (joins tbl_promo)
# ---------------------------------------------------------------------------

async def get_promo_usage(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    How many times each promo code was used and total discount given.
    tbl_visit.PromoCode is an int FK to tbl_promo.Id.

    Returns rows like:
        {
          "promo_id": 3,
          "promo_code": "SUMMER20",
          "times_used": 14,
          "total_discount": 280.0
        }
    """
    sql = """
        SELECT
            p.Id                              AS promo_id,
            p.PromoCode                       AS promo_code,
            COUNT(v.ID)                       AS times_used,
            ROUND(SUM(v.Discount), 2)         AS total_discount
        FROM tbl_visit v
        JOIN tbl_promo p ON v.PromoCode = p.Id
        WHERE v.OrganizationId  = %s
          AND v.RecDateTime     BETWEEN %s AND %s
          AND v.PaymentStatus   = 1
          AND v.PromoCode       IS NOT NULL
        GROUP BY p.Id, p.PromoCode
        ORDER BY times_used DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()