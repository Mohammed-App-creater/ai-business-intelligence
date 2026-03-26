"""
queries/forms.py
================
Intake form completion queries (Production DB).

Tables used:
  tbl_form     — form templates per org
  tbl_formcust — form assignments per customer per org

CRITICAL column note:
  tbl_formcust uses `Orgid` (lowercase 'i') — not `OrgId` or `OrganizationId`.
  Always use the exact case: Orgid

tbl_form columns:
  Id, Name, Description, OrgId, HtmlTemp, Active, CategoryId, RecDate

tbl_formcust columns:
  Id, FormId, CustId, Orgid, Active, Status, FormTemp, RecDate, OnlineCode

tbl_formcust.Status values (varchar):
  'ready'    — assigned, not yet submitted by customer
  'complete' — customer submitted the form
  'approved' — staff approved the submitted form
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Form catalog for an org
# ---------------------------------------------------------------------------

async def get_form_catalog(
    pool,
    org_id: int,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """
    All intake form templates for an org.

    Returns rows like:
        {
          "form_id": 4,
          "name": "New Client Intake",
          "description": "Standard new client form",
          "category_id": 1,
          "active": 1,
          "created_date": "2025-01-15"
        }
    """
    active_clause = "AND Active = 1" if active_only else ""
    sql = f"""
        SELECT
            Id            AS form_id,
            Name          AS name,
            Description   AS description,
            CategoryId    AS category_id,
            Active        AS active,
            DATE(RecDate) AS created_date
        FROM tbl_form
        WHERE OrgId = %s
          {active_clause}
        ORDER BY Name
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Form completion summary per form
# ---------------------------------------------------------------------------

async def get_form_completion_summary(
    pool,
    org_id: int,
) -> list[dict[str, Any]]:
    """
    For each form: ready (pending), complete, and approved counts.
    Uses Orgid (lowercase i) on tbl_formcust.

    Returns rows like:
        {
          "form_id": 4,
          "form_name": "New Client Intake",
          "total_assigned": 42,
          "ready_count": 8,
          "complete_count": 28,
          "approved_count": 6,
          "completion_rate_pct": 80.9
        }
    """
    sql = """
        SELECT
            f.Id                                                  AS form_id,
            f.Name                                                AS form_name,
            COUNT(fc.Id)                                          AS total_assigned,
            SUM(CASE WHEN fc.Status = 'ready'
                THEN 1 ELSE 0 END)                               AS ready_count,
            SUM(CASE WHEN fc.Status = 'complete'
                THEN 1 ELSE 0 END)                               AS complete_count,
            SUM(CASE WHEN fc.Status = 'approved'
                THEN 1 ELSE 0 END)                               AS approved_count,
            ROUND(
                SUM(CASE WHEN fc.Status IN ('complete', 'approved')
                    THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(fc.Id), 0),
                1
            )                                                     AS completion_rate_pct
        FROM tbl_form f
        LEFT JOIN tbl_formcust fc ON f.Id    = fc.FormId
                                  AND fc.Orgid = %s
        WHERE f.OrgId  = %s
          AND f.Active = 1
        GROUP BY f.Id, f.Name
        ORDER BY total_assigned DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, org_id))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Pending forms (assigned but not yet submitted)
# ---------------------------------------------------------------------------

async def get_pending_forms(
    pool,
    org_id: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Clients who have been assigned a form but haven't completed it.
    Used for "has this client filled out their intake form?" questions.

    Returns rows like:
        {
          "formcust_id": 22,
          "form_id": 4,
          "form_name": "New Client Intake",
          "cust_id": 441,
          "status": "ready",
          "assigned_date": "2026-03-01"
        }
    """
    sql = """
        SELECT
            fc.Id            AS formcust_id,
            fc.FormId        AS form_id,
            f.Name           AS form_name,
            fc.CustId        AS cust_id,
            fc.Status        AS status,
            DATE(fc.RecDate) AS assigned_date
        FROM tbl_formcust fc
        JOIN tbl_form f ON fc.FormId = f.Id
        WHERE fc.Orgid  = %s
          AND fc.Status = 'ready'
          AND fc.Active = 1
        ORDER BY fc.RecDate ASC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, limit))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Form completions per month
# ---------------------------------------------------------------------------

async def get_form_completions_by_month(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly count of form completions and approvals.

    Returns rows like:
        {"month": "2026-03", "completed": 18, "approved": 12, "total": 30}
    """
    sql = """
        SELECT
            DATE_FORMAT(fc.RecDate, '%%Y-%%m')              AS month,
            SUM(CASE WHEN fc.Status = 'complete'
                THEN 1 ELSE 0 END)                          AS completed,
            SUM(CASE WHEN fc.Status = 'approved'
                THEN 1 ELSE 0 END)                          AS approved,
            COUNT(fc.Id)                                    AS total
        FROM tbl_formcust fc
        WHERE fc.Orgid   = %s
          AND fc.RecDate BETWEEN %s AND %s
          AND fc.Status  IN ('complete', 'approved')
        GROUP BY DATE_FORMAT(fc.RecDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Form status for a specific client
# ---------------------------------------------------------------------------

async def get_client_form_status(
    pool,
    org_id: int,
    cust_id: int,
) -> list[dict[str, Any]]:
    """
    All forms assigned to a specific client and their status.
    LOOKUP route: "has client X completed their intake form?"

    Returns rows like:
        {
          "form_id": 4,
          "form_name": "New Client Intake",
          "status": "complete",
          "assigned_date": "2026-03-01"
        }
    """
    sql = """
        SELECT
            f.Id             AS form_id,
            f.Name           AS form_name,
            fc.Status        AS status,
            DATE(fc.RecDate) AS assigned_date
        FROM tbl_formcust fc
        JOIN tbl_form f ON fc.FormId = f.Id
        WHERE fc.Orgid  = %s
          AND fc.CustId = %s
          AND fc.Active = 1
        ORDER BY fc.RecDate DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, cust_id))
            return await cur.fetchall()
