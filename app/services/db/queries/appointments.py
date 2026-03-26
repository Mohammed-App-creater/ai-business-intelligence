"""
queries/appointments.py
=======================
Appointment and booking queries (Production DB).

Tables used:
  tbl_calendarevent — bookings/appointments
  tbl_custsignin    — walk-in queue entries

tbl_calendarevent flag logic (confirmed from schema):
  Active    = 1 → event is live (not cancelled)
  Active    = 0 → cancelled
  Confirmed = 1 → client confirmed attendance
  Complete  = 1 → service was completed / client was served
  Complete  = 0 → not yet completed

Derived statuses:
  Cancelled  : Active = 0
  No-show    : Active = 1 AND Confirmed = 1 AND Complete = 0
               AND StartDate < NOW()
  Completed  : Active = 1 AND Complete = 1
  Upcoming   : Active = 1 AND StartDate > NOW()
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Monthly appointment summary
# ---------------------------------------------------------------------------

async def get_appointment_summary(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly totals: total bookings, completed, cancelled, no-shows,
    cancellation rate, completion rate.

    Returns rows like:
        {
          "month": "2026-03",
          "total": 180,
          "completed": 150,
          "cancelled": 18,
          "no_show": 12,
          "cancellation_rate_pct": 10.0,
          "completion_rate_pct": 83.3
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(StartDate, '%%Y-%%m')                AS month,
            COUNT(*)                                          AS total,
            SUM(CASE WHEN Active = 1
                      AND Complete = 1
                THEN 1 ELSE 0 END)                           AS completed,
            SUM(CASE WHEN Active = 0
                THEN 1 ELSE 0 END)                           AS cancelled,
            SUM(CASE WHEN Active    = 1
                      AND Confirmed = 1
                      AND Complete  = 0
                      AND StartDate < NOW()
                THEN 1 ELSE 0 END)                           AS no_show,
            ROUND(
                SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                                AS cancellation_rate_pct,
            ROUND(
                SUM(CASE WHEN Active = 1 AND Complete = 1
                    THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                                AS completion_rate_pct
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(StartDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# No-show rate trend
# ---------------------------------------------------------------------------

async def get_noshow_rate(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Monthly no-show rate.
    No-show = Confirmed=1, Complete=0, Active=1, StartDate in the past.

    Returns rows like:
        {
          "month": "2026-03",
          "confirmed_bookings": 120,
          "no_shows": 14,
          "noshow_rate_pct": 11.7
        }
    """
    sql = """
        SELECT
            DATE_FORMAT(StartDate, '%%Y-%%m')               AS month,
            SUM(CASE WHEN Confirmed = 1 THEN 1 ELSE 0 END)  AS confirmed_bookings,
            SUM(CASE WHEN Confirmed = 1
                      AND Complete  = 0
                      AND Active    = 1
                      AND StartDate < NOW()
                THEN 1 ELSE 0 END)                          AS no_shows,
            ROUND(
                SUM(CASE WHEN Confirmed = 1
                          AND Complete  = 0
                          AND Active    = 1
                          AND StartDate < NOW()
                    THEN 1 ELSE 0 END)
                * 100.0
                / NULLIF(SUM(CASE WHEN Confirmed = 1
                                  THEN 1 ELSE 0 END), 0),
                1
            )                                               AS noshow_rate_pct
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
          AND Active         = 1
        GROUP BY DATE_FORMAT(StartDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Cancellation trend
# ---------------------------------------------------------------------------

async def get_cancellation_trend(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Month-by-month cancellation count and rate.

    Returns rows like:
        {"month": "2026-03", "total_bookings": 180, "cancelled": 18, "cancellation_rate_pct": 10.0}
    """
    sql = """
        SELECT
            DATE_FORMAT(StartDate, '%%Y-%%m')               AS month,
            COUNT(*)                                         AS total_bookings,
            SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END)     AS cancelled,
            ROUND(
                SUM(CASE WHEN Active = 0 THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0),
                1
            )                                               AS cancellation_rate_pct
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
        GROUP BY DATE_FORMAT(StartDate, '%%Y-%%m')
        ORDER BY month
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Peak hours / busiest booking times
# ---------------------------------------------------------------------------

async def get_peak_hours(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Busiest booking hours and days of the week.
    Uses StartDate — only counts active (non-cancelled) appointments.

    Returns rows like:
        {"day_of_week": "Friday", "hour_of_day": 14, "booking_count": 47}
    """
    sql = """
        SELECT
            DAYNAME(StartDate)   AS day_of_week,
            HOUR(StartDate)      AS hour_of_day,
            COUNT(*)             AS booking_count
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
          AND Active         = 1
        GROUP BY DAYNAME(StartDate), DAYOFWEEK(StartDate), HOUR(StartDate)
        ORDER BY booking_count DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Bookings per staff member
# ---------------------------------------------------------------------------

async def get_bookings_by_staff(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Total, completed, and cancelled bookings per staff member.

    Returns rows like:
        {
          "emp_id": 12,
          "employee_name": "Maria Garcia",
          "total_bookings": 95,
          "completed": 82,
          "cancelled": 8,
          "no_show": 5
        }
    """
    sql = """
        SELECT
            EmployeeId                                       AS emp_id,
            EmployeeName                                     AS employee_name,
            COUNT(*)                                         AS total_bookings,
            SUM(CASE WHEN Active = 1
                      AND Complete = 1
                THEN 1 ELSE 0 END)                          AS completed,
            SUM(CASE WHEN Active = 0
                THEN 1 ELSE 0 END)                          AS cancelled,
            SUM(CASE WHEN Active    = 1
                      AND Confirmed = 1
                      AND Complete  = 0
                      AND StartDate < NOW()
                THEN 1 ELSE 0 END)                          AS no_show
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
        GROUP BY EmployeeId, EmployeeName
        ORDER BY total_bookings DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Upcoming appointments
# ---------------------------------------------------------------------------

async def get_upcoming_appointments(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Next upcoming confirmed appointments for the org.
    Used for LOOKUP route (live operational data).

    Returns rows like:
        {
          "id": 6100,
          "start_date": "2026-03-27 10:00:00",
          "customer_name": "Jane Smith",
          "service_name": "Balayage",
          "employee_name": "Maria Garcia",
          "branch_name": "Main Location",
          "confirmed": 1
        }
    """
    sql = """
        SELECT
            Id              AS id,
            StartDate       AS start_date,
            EndDate         AS end_date,
            CustomerName    AS customer_name,
            ServiceName     AS service_name,
            EmployeeName    AS employee_name,
            BranchName      AS branch_name,
            Confirmed       AS confirmed
        FROM tbl_calendarevent
        WHERE OrganizationId = %s
          AND StartDate      BETWEEN %s AND %s
          AND Active         = 1
          AND Complete       = 0
        ORDER BY StartDate ASC
        LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date, limit))
            return await cur.fetchall()
