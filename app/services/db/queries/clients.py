"""
queries/clients.py
==================
Client retention and activity queries (Production DB).

Tables used:
  tbl_custorg    — org-scoped client records (CustID + OrgID, Points, Active)
  tbl_custsignin — walk-in queue records (Status: 0=waiting, 1=served)
  tbl_visit      — joined for spend/revenue per client

Key notes:
  tbl_custorg has 107K rows — always filter by OrgID.
  tbl_customers has 10M rows — NEVER query directly without going through
  tbl_custorg first. All client queries here go via tbl_custorg.

  tbl_custsignin.Status: 0 = not served, 1 = served
  tbl_custsignin.AppType: 1 = walk-in, 2 = app booking
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Active client count
# ---------------------------------------------------------------------------

async def get_active_client_count(
    pool,
    org_id: int,
) -> dict[str, Any]:
    """
    Total active vs inactive clients for the org.

    Returns a single dict like:
        {"active_count": 842, "inactive_count": 120, "total_count": 962}
    """
    sql = """
        SELECT
            SUM(CASE WHEN Active = 1 THEN 1 ELSE 0 END) AS active_count,
            SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END) AS inactive_count,
            COUNT(*)                                     AS total_count
        FROM tbl_custorg
        WHERE OrgID = %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            rows = await cur.fetchall()
            return rows[0] if rows else {}


# ---------------------------------------------------------------------------
# New vs returning clients per month
# ---------------------------------------------------------------------------

async def get_client_retention(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    New vs returning clients per month.
    "New" = first ever visit for that client at this org.
    "Returning" = had at least one prior visit.

    Uses tbl_visit for visit history — scoped by OrganizationId.

    Returns rows like:
        {
          "month": "2026-03",
          "new_clients": 34,
          "returning_clients": 98,
          "total_visits": 132
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(v.RecDateTime, '%%Y-%%m')        AS month,
            COUNT(DISTINCT CASE
                WHEN v.RecDateTime = first_visit.first_dt
                THEN v.CustID END)                       AS new_clients,
            COUNT(DISTINCT CASE
                WHEN v.RecDateTime > first_visit.first_dt
                THEN v.CustID END)                       AS returning_clients,
            COUNT(DISTINCT v.CustID)                     AS total_unique_clients
        FROM tbl_visit v
        JOIN (
            SELECT CustID, MIN(RecDateTime) AS first_dt
            FROM tbl_visit
            WHERE OrganizationId = %s
              AND PaymentStatus  = 1
            GROUP BY CustID
        ) first_visit ON v.CustID = first_visit.CustID
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY DATE_FORMAT(v.RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Lapsed clients
# ---------------------------------------------------------------------------

async def get_lapsed_clients(
    pool,
    org_id: int,
    days_since_visit: int = 60,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Clients who were active but haven't visited in X days.
    Good for "at-risk" / re-engagement targeting.

    Returns rows like:
        {
          "cust_id": 441,
          "total_visits": 8,
          "last_visit_date": "2025-12-01",
          "days_since_last_visit": 115,
          "total_spend": 640.0
        }
    """
    sql = """
        SELECT
            v.CustID                                    AS cust_id,
            COUNT(v.ID)                                 AS total_visits,
            DATE(MAX(v.RecDateTime))                    AS last_visit_date,
            DATEDIFF(CURDATE(), MAX(v.RecDateTime))     AS days_since_last_visit,
            ROUND(SUM(v.TotalPay), 2)                   AS total_spend
        FROM tbl_visit v
        JOIN tbl_custorg co ON v.CustID = co.CustID
                            AND co.OrgID = v.OrganizationId
        WHERE v.OrganizationId = %s
          AND v.PaymentStatus  = 1
          AND co.Active        = 1
        GROUP BY v.CustID
        HAVING DATEDIFF(CURDATE(), MAX(v.RecDateTime)) >= %s
        ORDER BY days_since_last_visit DESC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, days_since_visit, limit))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Top clients by spend
# ---------------------------------------------------------------------------

async def get_top_clients_by_spend(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Top clients ranked by total spend in a period.
    Joins tbl_custorg then tbl_customers for name — via tbl_custorg to avoid
    the 10M row tbl_customers full scan.

    Returns rows like:
        {
          "cust_id": 441,
          "first_name": "Jane",
          "last_name": "Smith",
          "visit_count": 12,
          "total_spend": 1440.0,
          "avg_spend_per_visit": 120.0,
          "loyalty_points": 144
        }
    """
    sql = """
        SELECT
            v.CustID                            AS cust_id,
            c.FirstName                         AS first_name,
            c.LastName                          AS last_name,
            COUNT(v.ID)                         AS visit_count,
            ROUND(SUM(v.TotalPay), 2)           AS total_spend,
            ROUND(AVG(v.TotalPay), 2)           AS avg_spend_per_visit,
            co.Points                           AS loyalty_points
        FROM tbl_visit v
        JOIN tbl_custorg  co ON v.CustID   = co.CustID
                             AND co.OrgID  = v.OrganizationId
        JOIN tbl_customers c ON v.CustID   = c.id
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY v.CustID, c.FirstName, c.LastName, co.Points
        ORDER BY total_spend DESC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date, limit))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Walk-in stats
# ---------------------------------------------------------------------------

async def get_walkin_stats(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Walk-in volume per month — served vs not served.
    tbl_custsignin.Status: 0 = waiting/not served, 1 = served.

    Returns rows like:
        {
          "month": "2026-03",
          "total_walkins": 142,
          "served": 128,
          "not_served": 14,
          "serve_rate_pct": 90.1
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(RecDateTime, '%%Y-%%m')           AS month,
            COUNT(*)                                      AS total_walkins,
            SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END)  AS served,
            SUM(CASE WHEN Status = 0 THEN 1 ELSE 0 END)  AS not_served,
            ROUND(
                SUM(CASE WHEN Status = 1 THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0),
                1
            )                                             AS serve_rate_pct
        FROM tbl_custsignin
        WHERE OrgId       = %s
          AND RecDateTime BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Client visit frequency distribution
# ---------------------------------------------------------------------------

async def get_visit_frequency_distribution(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    How often do clients visit? Buckets clients by visit count in a period.
    Useful for loyalty program targeting.

    Returns rows like:
        {"frequency_bucket": "1 visit",  "client_count": 120}
        {"frequency_bucket": "2-3 visits","client_count": 85}
        {"frequency_bucket": "4-6 visits","client_count": 40}
        {"frequency_bucket": "7+ visits", "client_count": 15}
    """
    sql = """
        SELECT
            CASE
                WHEN visit_count = 1          THEN '1 visit'
                WHEN visit_count BETWEEN 2 AND 3 THEN '2-3 visits'
                WHEN visit_count BETWEEN 4 AND 6 THEN '4-6 visits'
                ELSE '7+ visits'
            END                    AS frequency_bucket,
            COUNT(*)               AS client_count
        FROM (
            SELECT CustID, COUNT(*) AS visit_count
            FROM tbl_visit
            WHERE OrganizationId = %s
              AND RecDateTime    BETWEEN %s AND %s
              AND PaymentStatus  = 1
            GROUP BY CustID
        ) freq
        GROUP BY frequency_bucket
        ORDER BY FIELD(
            frequency_bucket,
            '1 visit', '2-3 visits', '4-6 visits', '7+ visits'
        )
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()
