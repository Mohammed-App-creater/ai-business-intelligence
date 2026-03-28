"""Staff performance extractor — merged from visits, tips, calendar, reviews."""
from __future__ import annotations

from datetime import date

from scripts.etl.base import BaseExtractor
from scripts.etl.extractors._util import period_end_exclusive

_SQL_VISITS = """
SELECT
    e.OrganizationId                            AS business_id,
    sv.EmpID                                    AS employee_id,
    MAX(TRIM(CONCAT(e.FirstName, ' ', COALESCE(e.LastName,'')))) AS employee_name,
    DATE_FORMAT(v.RecDateTime, '%Y-%m-01')    AS period_start,
    LAST_DAY(v.RecDateTime)                     AS period_end,
    COUNT(DISTINCT sv.VisitID)                  AS total_visits,
    SUM(sv.ServicePrice)                        AS total_revenue,
    SUM(sv.EmpCom + sv.ServCom)                AS total_commission
FROM tbl_service_visit sv
JOIN tbl_visit v  ON sv.VisitID = v.ID
JOIN tbl_emp   e  ON sv.EmpID   = e.id
WHERE e.OrganizationId = %s
  AND v.RecDateTime >= %s
  AND v.RecDateTime <  %s
GROUP BY e.OrganizationId, sv.EmpID, DATE_FORMAT(v.RecDateTime, '%Y-%m-01')
""".strip()

_SQL_TIPS = """
SELECT
    v.EmpID                                     AS employee_id,
    DATE_FORMAT(v.RecDateTime, '%Y-%m-01')    AS period_start,
    MAX(LAST_DAY(v.RecDateTime))                AS period_end,
    SUM(v.Tips)                                 AS total_tips
FROM tbl_visit v
WHERE v.OrganizationId = %s
  AND v.RecDateTime >= %s
  AND v.RecDateTime <  %s
GROUP BY v.EmpID, DATE_FORMAT(v.RecDateTime, '%Y-%m-01')
""".strip()

_SQL_APPOINTMENTS = """
SELECT
    OrganizationId                              AS business_id,
    EmployeeId                                  AS employee_id,
    DATE_FORMAT(StartDate, '%Y-%m-01')        AS period_start,
    LAST_DAY(StartDate)                         AS period_end,
    COUNT(*)                                    AS appointments_booked,
    SUM(CASE WHEN Complete = 1 THEN 1 ELSE 0 END) AS appointments_completed,
    SUM(CASE WHEN Active   = 0 THEN 1 ELSE 0 END) AS appointments_cancelled
FROM tbl_calendarevent
WHERE OrganizationId = %s
  AND StartDate >= %s
  AND StartDate <  %s
GROUP BY OrganizationId, EmployeeId, DATE_FORMAT(StartDate, '%Y-%m-01')
""".strip()

_SQL_RATINGS = """
SELECT
    e.OrganizationId                            AS business_id,
    r.employee_id                               AS employee_id,
    DATE_FORMAT(r.created_at, '%Y-%m-01')    AS period_start,
    LAST_DAY(r.created_at)                      AS period_end,
    AVG(r.rating)                               AS avg_rating,
    COUNT(*)                                    AS review_count
FROM tbl_emp_reviews r
JOIN tbl_emp e ON r.employee_id = e.id
WHERE e.OrganizationId = %s
  AND r.created_at >= %s
  AND r.created_at <  %s
  AND r.rating IS NOT NULL
GROUP BY e.OrganizationId, r.employee_id, DATE_FORMAT(r.created_at, '%Y-%m-01')
""".strip()


def _emp_period_key(row: dict) -> tuple:
    return (row["employee_id"], row["period_start"])


class StaffExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        params = (org_id, period_start, end_excl)
        rows_a = await self.fetch_all(_SQL_VISITS, params)
        tips_rows = await self.fetch_all(_SQL_TIPS, params)
        appt_rows = await self.fetch_all(_SQL_APPOINTMENTS, params)
        rate_rows = await self.fetch_all(_SQL_RATINGS, params)

        merged: dict[tuple, dict] = {}
        for r in rows_a:
            k = _emp_period_key(r)
            row = dict(r)
            row["total_tips"] = 0.0
            row.setdefault("appointments_booked", 0)
            row.setdefault("appointments_completed", 0)
            row.setdefault("appointments_cancelled", 0)
            row.setdefault("avg_rating", None)
            row.setdefault("review_count", 0)
            merged[k] = row

        for r in tips_rows:
            k = _emp_period_key(r)
            if k in merged:
                merged[k]["total_tips"] = r.get("total_tips") or 0.0
            else:
                merged[k] = {
                    "business_id": org_id,
                    "employee_id": r["employee_id"],
                    "employee_name": "Unknown",
                    "period_start": r["period_start"],
                    "period_end": r["period_end"],
                    "total_visits": 0,
                    "total_revenue": 0.0,
                    "total_tips": float(r.get("total_tips") or 0),
                    "total_commission": 0.0,
                    "appointments_booked": 0,
                    "appointments_completed": 0,
                    "appointments_cancelled": 0,
                    "avg_rating": None,
                    "review_count": 0,
                }

        def _defaults_from_appt(r: dict) -> dict:
            return {
                "business_id": r["business_id"],
                "employee_id": r["employee_id"],
                "employee_name": "Unknown",
                "period_start": r["period_start"],
                "period_end": r["period_end"],
                "total_visits": 0,
                "total_revenue": 0.0,
                "total_tips": 0.0,
                "total_commission": 0.0,
                "appointments_booked": r.get("appointments_booked", 0),
                "appointments_completed": r.get("appointments_completed", 0),
                "appointments_cancelled": r.get("appointments_cancelled", 0),
                "avg_rating": None,
                "review_count": 0,
            }

        for r in appt_rows:
            k = _emp_period_key(r)
            if k not in merged:
                merged[k] = _defaults_from_appt(r)
            else:
                merged[k]["appointments_booked"] = r.get("appointments_booked", 0)
                merged[k]["appointments_completed"] = r.get("appointments_completed", 0)
                merged[k]["appointments_cancelled"] = r.get("appointments_cancelled", 0)

        for r in rate_rows:
            k = _emp_period_key(r)
            if k not in merged:
                merged[k] = {
                    "business_id": r["business_id"],
                    "employee_id": r["employee_id"],
                    "employee_name": "Unknown",
                    "period_start": r["period_start"],
                    "period_end": r["period_end"],
                    "total_visits": 0,
                    "total_revenue": 0.0,
                    "total_tips": 0.0,
                    "total_commission": 0.0,
                    "appointments_booked": 0,
                    "appointments_completed": 0,
                    "appointments_cancelled": 0,
                    "avg_rating": r.get("avg_rating"),
                    "review_count": r.get("review_count", 0),
                }
            else:
                merged[k]["avg_rating"] = r.get("avg_rating")
                merged[k]["review_count"] = r.get("review_count", 0)

        return list(merged.values())
