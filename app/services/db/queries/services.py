"""
queries/services.py
===================
Service popularity queries against tbl_service_visit, tbl_visit,
tbl_service (Production DB).

CRITICAL JOIN NOTE:
  tbl_service_visit has NO OrganizationId column.
  Tenant scoping MUST go through tbl_visit:
      tbl_service_visit sv
      JOIN tbl_visit v ON sv.VisitID = v.ID
      WHERE v.OrganizationId = %s

Key columns:
  tbl_service_visit:
    ServiceID     — FK → tbl_service.id
    EmpID         — staff who performed it
    VisitID       — FK → tbl_visit.ID
    ServicePrice  — price charged (may differ from tbl_service.Price)
    EmpCom        — employee commission amount
    ServCom       — service-level commission amount

  tbl_service:
    id, Name, Price, Duration, OrganizationId, Active, categoryid
    Product — 1=service, 0=product
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Service popularity — bookings and revenue
# ---------------------------------------------------------------------------

async def get_service_popularity(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Top services ranked by booking count and revenue.

    Returns rows like:
        {
          "service_id": 5,
          "service_name": "Balayage",
          "unit_price": 120.0,
          "booking_count": 142,
          "revenue": 17040.0,
          "avg_price_charged": 120.0
        }
    """
    sql = """
        SELECT
            s.id                              AS service_id,
            s.Name                            AS service_name,
            s.Price                           AS unit_price,
            COUNT(sv.id)                      AS booking_count,
            ROUND(SUM(sv.ServicePrice), 2)    AS revenue,
            ROUND(AVG(sv.ServicePrice), 2)    AS avg_price_charged
        FROM tbl_service_visit sv
        JOIN tbl_visit   v ON sv.VisitID   = v.ID
        JOIN tbl_service s ON sv.ServiceID = s.id
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY s.id, s.Name, s.Price
        ORDER BY booking_count DESC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date, limit))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Service popularity by month (trend)
# ---------------------------------------------------------------------------

async def get_service_popularity_trend(
    pool,
    org_id: int,
    service_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly booking trend for a specific service.
    Used to answer "is X service growing or declining?"

    Returns rows like:
        {"month": "2026-03", "booking_count": 42, "revenue": 5040.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(v.RecDateTime, '%%Y-%%m')  AS month,
            COUNT(sv.id)                           AS booking_count,
            ROUND(SUM(sv.ServicePrice), 2)         AS revenue
        FROM tbl_service_visit sv
        JOIN tbl_visit v ON sv.VisitID = v.ID
        WHERE v.OrganizationId = %s
          AND sv.ServiceID     = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY DATE_FORMAT(v.RecDateTime, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, service_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Services per staff member
# ---------------------------------------------------------------------------

async def get_services_by_staff(
    pool,
    org_id: int,
    emp_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Which services a specific staff member performed and how many times.

    Returns rows like:
        {
          "service_id": 5,
          "service_name": "Balayage",
          "booking_count": 38,
          "revenue": 4560.0
        }
    """
    sql = """
        SELECT
            s.id                           AS service_id,
            s.Name                         AS service_name,
            COUNT(sv.id)                   AS booking_count,
            ROUND(SUM(sv.ServicePrice), 2) AS revenue
        FROM tbl_service_visit sv
        JOIN tbl_visit   v ON sv.VisitID   = v.ID
        JOIN tbl_service s ON sv.ServiceID = s.id
        WHERE v.OrganizationId = %s
          AND sv.EmpID         = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
        GROUP BY s.id, s.Name
        ORDER BY booking_count DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, emp_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Walk-in service demand (tbl_signinservice + tbl_custsignin)
# ---------------------------------------------------------------------------

async def get_walkin_service_demand(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """
    Which services walk-in customers request most often.
    Uses tbl_signinservice (no OrgId) joined to tbl_custsignin (has OrgId).

    Returns rows like:
        {"service_id": 3, "service_name": "Haircut", "request_count": 87}
    """
    sql = """
        SELECT
            s.id                AS service_id,
            s.Name              AS service_name,
            COUNT(ss.Id)        AS request_count
        FROM tbl_signinservice ss
        JOIN tbl_custsignin cs ON ss.CustSigninId = cs.Id
        JOIN tbl_service    s  ON ss.ServiceId    = s.id
        WHERE cs.OrgId       = %s
          AND cs.RecDateTime BETWEEN %s AND %s
        GROUP BY s.id, s.Name
        ORDER BY request_count DESC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date, limit))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Service catalog for the org
# ---------------------------------------------------------------------------

async def get_service_catalog(
    pool,
    org_id: int,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """
    All services offered by the org.
    Use when answering "what services do we have?" or for name lookups.

    Returns rows like:
        {
          "service_id": 5,
          "name": "Balayage",
          "price": 120.0,
          "duration": 90,
          "category_id": 3,
          "active": 1
        }
    """
    active_clause = "AND Active = 1" if active_only else ""
    sql = f"""
        SELECT
            id          AS service_id,
            Name        AS name,
            Price       AS price,
            Duration    AS duration,
            categoryid  AS category_id,
            Active      AS active
        FROM tbl_service
        WHERE OrganizationId = %s
          {active_clause}
        ORDER BY Name
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Service inventory levels
# ---------------------------------------------------------------------------

async def get_service_inventory(
    pool,
    org_id: int,
) -> list[dict[str, Any]]:
    """
    Stock/inventory levels per service per location.
    From tbl_service_location_inventory.

    Returns rows like:
        {"service_id": 5, "location_id": 2, "quantity": 12}
    """
    sql = """
        SELECT
            sli.ServiceId   AS service_id,
            s.Name          AS service_name,
            sli.LocationId  AS location_id,
            sli.Quantity    AS quantity
        FROM tbl_service_location_inventory sli
        JOIN tbl_service s ON sli.ServiceId = s.id
        WHERE s.OrganizationId = %s
          AND s.Active         = 1
        ORDER BY s.Name, sli.LocationId
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            return await cur.fetchall()
