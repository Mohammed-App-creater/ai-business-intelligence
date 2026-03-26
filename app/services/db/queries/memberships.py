"""
queries/memberships.py
======================
Client subscription/membership queries (Production DB).

Table used:
  tbl_custsubscription — client membership records per org

Key columns:
  CustId          — FK → tbl_customers.id
  OrgId           — tenant scoping
  LocId           — location
  ServiceID       — which service the subscription is for
  Amount          — subscription price
  Discount        — discount applied
  SubCreateDate   — when subscription was created
  SubExecutionDate — next billing/execution date
  Active          — 1 = active, 0 = cancelled/inactive
  Interval        — billing interval in days (default 30)

Note: Individual client subscription lookups (is this client subscribed?)
go via the SaaS API. These DB queries handle aggregations:
how many subscriptions, revenue from memberships, churn trends.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Active subscription count
# ---------------------------------------------------------------------------

async def get_active_subscription_count(
    pool,
    org_id: int,
) -> dict[str, Any]:
    """
    Total active vs inactive subscriptions and monthly recurring revenue.

    Returns a single dict like:
        {
          "active_count": 42,
          "inactive_count": 8,
          "total_count": 50,
          "monthly_recurring_revenue": 2100.0,
          "avg_subscription_amount": 50.0
        }
    """
    sql = """
        SELECT
            SUM(CASE WHEN Active = 1 THEN 1 ELSE 0 END)          AS active_count,
            SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END)          AS inactive_count,
            COUNT(*)                                              AS total_count,
            ROUND(
                SUM(CASE WHEN Active = 1
                    THEN (Amount - Discount) ELSE 0 END), 2
            )                                                     AS monthly_recurring_revenue,
            ROUND(AVG(CASE WHEN Active = 1
                THEN Amount ELSE NULL END), 2)                    AS avg_subscription_amount
        FROM tbl_custsubscription
        WHERE OrgId = %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            rows = await cur.fetchall()
            return rows[0] if rows else {}


# ---------------------------------------------------------------------------
# Monthly subscription growth
# ---------------------------------------------------------------------------

async def get_subscription_growth(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    New subscriptions created per month — growth trend.

    Returns rows like:
        {"month": "2026-03", "new_subscriptions": 8, "total_value": 400.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(SubCreateDate, '%%Y-%%m')   AS month,
            COUNT(*)                                AS new_subscriptions,
            ROUND(SUM(Amount - Discount), 2)        AS total_value
        FROM tbl_custsubscription
        WHERE OrgId        = %s
          AND SubCreateDate BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(SubCreateDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Subscriptions by service
# ---------------------------------------------------------------------------

async def get_subscriptions_by_service(
    pool,
    org_id: int,
) -> list[dict[str, Any]]:
    """
    Which services have the most active subscriptions.

    Returns rows like:
        {
          "service_id": 5,
          "service_name": "Balayage Monthly",
          "active_count": 18,
          "total_monthly_value": 900.0
        }
    """
    sql = """
        SELECT
            cs.ServiceID                            AS service_id,
            s.Name                                  AS service_name,
            SUM(CASE WHEN cs.Active = 1
                THEN 1 ELSE 0 END)                  AS active_count,
            ROUND(
                SUM(CASE WHEN cs.Active = 1
                    THEN cs.Amount - cs.Discount
                    ELSE 0 END), 2
            )                                       AS total_monthly_value
        FROM tbl_custsubscription cs
        JOIN tbl_service s ON cs.ServiceID = s.id
        WHERE cs.OrgId = %s
        GROUP BY cs.ServiceID, s.Name
        ORDER BY active_count DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Upcoming subscription renewals
# ---------------------------------------------------------------------------

async def get_upcoming_renewals(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Subscriptions due for renewal/execution in a date window.
    Used for cash flow planning and client follow-up.

    Returns rows like:
        {
          "subscription_id": 22,
          "cust_id": 441,
          "service_id": 5,
          "amount": 50.0,
          "discount": 0.0,
          "execution_date": "2026-04-01",
          "interval_days": 30
        }
    """
    sql = """
        SELECT
            Id              AS subscription_id,
            CustId          AS cust_id,
            ServiceID       AS service_id,
            Amount          AS amount,
            Discount        AS discount,
            SubExecutionDate AS execution_date,
            Interval        AS interval_days
        FROM tbl_custsubscription
        WHERE OrgId             = %s
          AND Active            = 1
          AND SubExecutionDate  BETWEEN %s AND %s
        ORDER BY SubExecutionDate ASC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Subscription cancellations per month
# ---------------------------------------------------------------------------

async def get_subscription_cancellations(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Cancelled subscriptions per month (Active=0) by creation date window.
    Useful for churn tracking.

    Returns rows like:
        {"month": "2026-03", "cancelled_count": 3, "lost_monthly_value": 150.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(SubCreateDate, '%%Y-%%m')   AS month,
            COUNT(*)                                AS cancelled_count,
            ROUND(SUM(Amount - Discount), 2)        AS lost_monthly_value
        FROM tbl_custsubscription
        WHERE OrgId        = %s
          AND Active       = 0
          AND SubCreateDate BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(SubCreateDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()
