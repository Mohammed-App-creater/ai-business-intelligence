"""Attendance raw-record extractor."""
from __future__ import annotations

from datetime import date

from scripts.etl.base import BaseExtractor
from scripts.etl.extractors._util import period_end_exclusive

_SQL = """
SELECT
    e.OrganizationId                                AS business_id,
    a.EmpID                                         AS employee_id,
    TRIM(CONCAT(e.FirstName, ' ',
         COALESCE(e.LastName, '')))                 AS employee_name,
    a.LocationID                                    AS location_id,
    DATE_FORMAT(a.RecTimeDate, '%Y-%m-01')        AS period_start,
    LAST_DAY(a.RecTimeDate)                         AS period_end,
    DATE(a.RecTimeDate)                             AS record_date,
    a.time_sign_in,
    a.time_sign_out
FROM tbl_attendance a
JOIN tbl_emp e ON a.EmpID = e.id
WHERE e.OrganizationId = %s
  AND a.RecTimeDate >= %s
  AND a.RecTimeDate <  %s
ORDER BY a.EmpID, a.RecTimeDate
""".strip()


class AttendanceExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        return await self.fetch_all(_SQL, (org_id, period_start, end_excl))
