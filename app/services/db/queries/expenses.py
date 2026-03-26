"""
queries/expenses.py
===================
Expense queries against tbl_expense, tbl_expense_category,
tbl_expense_subcategory (Production DB).

Key columns — tbl_expense:
  ID, OrganizationId, CategoryID, SubcategoryID,
  Amount, ExpenseDate, LocationID, isDeleted
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Monthly expenses broken down by category / subcategory
# ---------------------------------------------------------------------------

async def get_monthly_expenses(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly expense detail joined to category and subcategory tables.

    Returns rows like:
        {
          "month": "2026-01",
          "category": "Supplies",
          "subcategory": "Hair Products",
          "expense_count": 8,
          "total": 1200.0
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(e.ExpenseDate, '%%Y-%%m')  AS month,
            ec.Name                                AS Category,
            es.Name                                AS subcategory,
            COUNT(e.ID)                            AS expense_count,
            ROUND(SUM(e.Amount), 2)                AS total
        FROM tbl_expense e
        JOIN tbl_expense_category    ec ON e.CategoryID    = ec.ID
        JOIN tbl_expense_subcategory es ON e.SubcategoryID = es.ID
        WHERE e.OrganizationId = %s
          AND e.ExpenseDate BETWEEN %s AND %s
          AND e.isDeleted = 0
        GROUP BY month, Category, subcategory
        ORDER BY month, total DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Monthly expense totals (no category join — fast roll-up)
# ---------------------------------------------------------------------------

async def get_monthly_expense_totals(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Total expenses per month without category breakdown.

    Returns rows like:
        {"month": "2026-01", "expense_count": 20, "total": 3200.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(e.ExpenseDate, '%%Y-%%m')  AS month,
            COUNT(e.ID)                            AS expense_count,
            ROUND(SUM(e.Amount), 2)                AS total
        FROM tbl_expense e
        WHERE e.OrganizationId = %s
          AND e.ExpenseDate BETWEEN %s AND %s
          AND e.isDeleted = 0
        GROUP BY DATE_FORMAT(e.ExpenseDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Net profit — revenue minus expenses per month
# ---------------------------------------------------------------------------

async def get_net_profit(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly net profit: revenue (tbl_visit) minus expenses (tbl_expense).

    Uses a UNION of both tables to build a complete month spine so months
    with only revenue or only expenses still appear.

    Returns rows like:
        {"month": "2026-01", "revenue": 12000.0, "expenses": 3200.0, "net_profit": 8800.0}

    Params repeated 4 times (org_id, from_date, to_date) × 4 subqueries = 12 total.
    """
    sql = """
        SELECT
            months.month,
            COALESCE(r.revenue,  0) AS revenue,
            COALESCE(e.expenses, 0) AS expenses,
            ROUND(COALESCE(r.revenue, 0) - COALESCE(e.expenses, 0), 2) AS net_profit
        FROM (
            SELECT DATE_FORMAT(RecDateTime, '%%Y-%%m') AS month
            FROM tbl_visit
            WHERE OrganizationId = %s
              AND RecDateTime BETWEEN %s AND %s
            UNION
            SELECT DATE_FORMAT(ExpenseDate, '%%Y-%%m') AS month
            FROM tbl_expense
            WHERE OrganizationId = %s
              AND ExpenseDate BETWEEN %s AND %s
        ) months
        LEFT JOIN (
            SELECT
                DATE_FORMAT(RecDateTime, '%%Y-%%m') AS month,
                ROUND(SUM(Total), 2)                AS revenue
            FROM tbl_visit
            WHERE OrganizationId = %s
              AND RecDateTime BETWEEN %s AND %s
              AND PaymentStatus = 1
            GROUP BY DATE_FORMAT(RecDateTime, '%%Y-%%m')
        ) r ON months.month = r.month
        LEFT JOIN (
            SELECT
                DATE_FORMAT(ExpenseDate, '%%Y-%%m') AS month,
                ROUND(SUM(Amount), 2)               AS expenses
            FROM tbl_expense
            WHERE OrganizationId = %s
              AND ExpenseDate BETWEEN %s AND %s
              AND isDeleted = 0
            GROUP BY DATE_FORMAT(ExpenseDate, '%%Y-%%m')
        ) e ON months.month = e.month
        ORDER BY months.month
    """
    params = (
        org_id, from_date, to_date,   # UNION — tbl_visit
        org_id, from_date, to_date,   # UNION — tbl_expense
        org_id, from_date, to_date,   # LEFT JOIN revenue
        org_id, from_date, to_date,   # LEFT JOIN expenses
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Expenses broken down by category with % of total
# ---------------------------------------------------------------------------

async def get_expenses_by_category(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Expense totals per category with each category's share of the grand total.

    Returns rows like:
        {"category": "Supplies", "expense_count": 15, "total": 2400.0, "pct_of_total": 45.2}
    """
    sql = """
        SELECT
            ec.Name                                                   AS category,
            COUNT(e.ID)                                               AS expense_count,
            ROUND(SUM(e.Amount), 2)                                   AS total,
            ROUND(
                100.0 * SUM(e.Amount) / SUM(SUM(e.Amount)) OVER (),
                1
            )                                                         AS pct_of_total
        FROM tbl_expense e
        JOIN tbl_expense_category ec ON e.CategoryID = ec.ID
        WHERE e.OrganizationId = %s
          AND e.ExpenseDate BETWEEN %s AND %s
          AND e.isDeleted = 0
        GROUP BY ec.Name
        ORDER BY total DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Expenses broken down by location
# ---------------------------------------------------------------------------

async def get_expenses_by_location(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Expense totals grouped by location.

    Returns rows like:
        {"location_id": 1, "expense_count": 18, "total": 1800.0}
    """
    sql = """
        SELECT
            e.LocationID                AS location_id,
            COUNT(e.ID)                 AS expense_count,
            ROUND(SUM(e.Amount), 2)     AS total
        FROM tbl_expense e
        WHERE e.OrganizationId = %s
          AND e.ExpenseDate BETWEEN %s AND %s
          AND e.isDeleted = 0
        GROUP BY LocationID
        ORDER BY total DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()
