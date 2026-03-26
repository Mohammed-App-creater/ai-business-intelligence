"""
queries/staff.py
================
Staff performance queries (Production DB).

Tables used:
  tbl_emp           — employee roster, name, hire date, active flag
  tbl_emp_reviews   — internal post-visit ratings (visit_id, employee_id, rating)
  tbl_visit_review  — visit-level reviews (OrganizationId scoped directly)
  tbl_attendance    — clock-in/out records (time_sign_in/out are VARCHAR, not datetime)
  tbl_empcom        — commission structure per staff per service
  tbl_google_review — Google reviews synced per org/location

CRITICAL — tbl_attendance time fields:
  time_sign_in / time_sign_out are VARCHAR(50) with default '0'.
  We use NULLIF to exclude the sentinel '0' value, and TIMEDIFF / TIME_TO_SEC
  to compute hours. Rows where either field is '0' are excluded from hour calcs.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Staff performance — revenue, visits, avg ticket
# ---------------------------------------------------------------------------

async def get_staff_performance(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Revenue, visit count, avg ticket, and tips per active staff member.

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
            e.id                            AS emp_id,
            e.FirstName                     AS first_name,
            e.LastName                      AS last_name,
            COUNT(v.ID)                     AS visit_count,
            ROUND(SUM(v.TotalPay),  2)      AS total_revenue,
            ROUND(AVG(v.TotalPay),  2)      AS avg_ticket,
            ROUND(SUM(v.Tips),      2)      AS tips
        FROM tbl_visit v
        JOIN tbl_emp e ON v.EmpID = e.id
        WHERE v.OrganizationId = %s
          AND v.RecDateTime    BETWEEN %s AND %s
          AND v.PaymentStatus  = 1
          AND e.Active         = 1
        GROUP BY e.id, e.FirstName, e.LastName
        ORDER BY total_revenue DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Staff internal ratings (tbl_emp_reviews)
# ---------------------------------------------------------------------------

async def get_staff_ratings(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Average rating and review count per staff member from internal reviews.
    tbl_emp_reviews links via visit_id → tbl_visit for org scoping.

    Returns rows like:
        {
          "emp_id": 12,
          "first_name": "Maria",
          "last_name": "Garcia",
          "review_count": 42,
          "avg_rating": 4.8,
          "five_star_count": 35,
          "one_star_count": 1
        }
    """
    sql = """
        SELECT
            e.id                                        AS emp_id,
            e.FirstName                                 AS first_name,
            e.LastName                                  AS last_name,
            COUNT(r.id)                                 AS review_count,
            ROUND(AVG(r.rating), 2)                     AS avg_rating,
            SUM(CASE WHEN r.rating = 5 THEN 1 ELSE 0 END) AS five_star_count,
            SUM(CASE WHEN r.rating = 1 THEN 1 ELSE 0 END) AS one_star_count
        FROM tbl_emp_reviews r
        JOIN tbl_visit v ON r.visit_id   = v.ID
        JOIN tbl_emp   e ON r.employee_id = e.id
        WHERE v.OrganizationId = %s
          AND r.created_at    BETWEEN %s AND %s
        GROUP BY e.id, e.FirstName, e.LastName
        ORDER BY avg_rating DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Visit-level reviews (tbl_visit_review — org scoped directly)
# ---------------------------------------------------------------------------

async def get_visit_reviews(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Visit-level review summary per org.
    tbl_visit_review has OrganizationId directly — no join needed.

    Returns rows like:
        {
          "review_count": 87,
          "avg_rating": 4.6,
          "five_star_count": 60,
          "four_star_count": 18,
          "three_star_count": 5,
          "two_star_count": 2,
          "one_star_count": 2
        }
    """
    sql = """
        SELECT
            COUNT(Id)                                      AS review_count,
            ROUND(AVG(Rating), 2)                          AS avg_rating,
            SUM(CASE WHEN Rating = 5 THEN 1 ELSE 0 END)   AS five_star_count,
            SUM(CASE WHEN Rating = 4 THEN 1 ELSE 0 END)   AS four_star_count,
            SUM(CASE WHEN Rating = 3 THEN 1 ELSE 0 END)   AS three_star_count,
            SUM(CASE WHEN Rating = 2 THEN 1 ELSE 0 END)   AS two_star_count,
            SUM(CASE WHEN Rating = 1 THEN 1 ELSE 0 END)   AS one_star_count
        FROM tbl_visit_review
        WHERE OrganizationId = %s
          AND CreatedAt      BETWEEN %s AND %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            rows = await cur.fetchall()
            return rows


# ---------------------------------------------------------------------------
# Staff hours worked (tbl_attendance)
# ---------------------------------------------------------------------------

async def get_staff_hours(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Hours worked per staff member from attendance records.

    IMPORTANT: time_sign_in / time_sign_out are VARCHAR(50) with sentinel '0'.
    We exclude rows where either field is '0' or NULL.
    TIMEDIFF computes duration; TIME_TO_SEC / 3600 converts to decimal hours.

    Returns rows like:
        {
          "emp_id": 12,
          "first_name": "Maria",
          "last_name": "Garcia",
          "days_attended": 22,
          "total_hours": 176.5
        }
    """
    sql = """
        SELECT
            e.id                                   AS emp_id,
            e.FirstName                            AS first_name,
            e.LastName                             AS last_name,
            COUNT(a.id)                            AS days_attended,
            ROUND(
                SUM(
                    TIME_TO_SEC(
                        TIMEDIFF(
                            NULLIF(a.time_sign_out, '0'),
                            NULLIF(a.time_sign_in,  '0')
                        )
                    ) / 3600.0
                ), 2
            )                                      AS total_hours
        FROM tbl_attendance a
        JOIN tbl_emp e ON a.EmpID = e.id
        WHERE e.OrganizationId   = %s
          AND a.RecTimeDate      BETWEEN %s AND %s
          AND a.time_sign_in    != '0'
          AND a.time_sign_out   != '0'
        GROUP BY e.id, e.FirstName, e.LastName
        ORDER BY total_hours DESC
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Staff commission structure (tbl_empcom)
# ---------------------------------------------------------------------------

async def get_staff_commission_structure(
    pool,
    org_id: int,
    emp_id: int | None = None,
) -> list[dict[str, Any]]:
    """
    Commission rates per staff member per service.
    commissionType: '%' = percentage, '$' = flat amount.

    Returns rows like:
        {
          "emp_id": 12,
          "first_name": "Maria",
          "service_id": 5,
          "service_name": "Balayage",
          "commission_type": "%",
          "commission": 30.0
        }
    """
    emp_clause = "AND ec.Emp_id = %s" if emp_id is not None else ""
    params = [org_id]
    if emp_id is not None:
        params.append(emp_id)

    sql = f"""
        SELECT
            e.id                  AS emp_id,
            e.FirstName           AS first_name,
            e.LastName            AS last_name,
            s.id                  AS service_id,
            s.Name                AS service_name,
            ec.commissionType     AS commission_type,
            ec.commission         AS commission
        FROM tbl_empcom ec
        JOIN tbl_emp     e ON ec.Emp_id     = e.id
        JOIN tbl_service s ON ec.Service_id = s.id
        WHERE ec.OrganizationId = %s
          AND ec.Active         = '1'
          {emp_clause}
        ORDER BY e.FirstName, s.Name
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Google reviews (tbl_google_review)
# ---------------------------------------------------------------------------

async def get_google_reviews_summary(
    pool,
    org_id: int,
    from_date: str | datetime,
    to_date: str | datetime,
) -> list[dict[str, Any]]:
    """
    Google review summary per location — avg rating, count, bad review flag count.
    tbl_google_review has organization_id and location_id directly.

    Returns rows like:
        {
          "location_id": 1,
          "review_count": 24,
          "avg_rating": 4.3,
          "bad_review_count": 2,
          "replied_count": 18
        }
    """
    sql = """
        SELECT
            location_id,
            COUNT(id)                                         AS review_count,
            ROUND(AVG(rating), 2)                             AS avg_rating,
            SUM(CASE WHEN is_bad_review = 1 THEN 1 ELSE 0 END) AS bad_review_count,
            SUM(CASE WHEN our_reply_text IS NOT NULL
                     THEN 1 ELSE 0 END)                       AS replied_count
        FROM tbl_google_review
        WHERE organization_id = %s
          AND review_time     BETWEEN %s AND %s
        GROUP BY location_id
        ORDER BY location_id
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id, from_date, to_date))
            return await cur.fetchall()


# ---------------------------------------------------------------------------
# Staff roster
# ---------------------------------------------------------------------------

async def get_staff_roster(
    pool,
    org_id: int,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """
    All staff for an org — name, hire date, role, active status.
    Used as a lookup / reference list.

    Returns rows like:
        {
          "emp_id": 12,
          "first_name": "Maria",
          "last_name": "Garcia",
          "hire_date": "2022-03-15",
          "role_id": 3,
          "active": 1
        }
    """
    active_clause = "AND Active = 1" if active_only else ""
    sql = f"""
        SELECT
            id          AS emp_id,
            FirstName   AS first_name,
            LastName    AS last_name,
            HireDate    AS hire_date,
            RoleId      AS role_id,
            Active      AS active
        FROM tbl_emp
        WHERE OrganizationId = %s
          {active_clause}
        ORDER BY FirstName, LastName
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (org_id,))
            return await cur.fetchall()
