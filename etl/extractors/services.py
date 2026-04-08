"""Service performance extractor."""
from __future__ import annotations

from datetime import date

from etl.base import BaseExtractor
from etl.extractors._util import period_end_exclusive

_SQL = """
SELECT
    v.OrganizationId                            AS business_id,
    sv.ServiceID                                AS service_id,
    COALESCE(s.Name, 'Unknown Service')         AS service_name,
    DATE_FORMAT(v.RecDateTime, '%Y-%m-01')    AS period_start,
    LAST_DAY(v.RecDateTime)                     AS period_end,
    COUNT(sv.id)                                AS booking_count,
    SUM(sv.ServicePrice)                        AS revenue,
    MIN(sv.ServicePrice)                        AS min_price,
    MAX(sv.ServicePrice)                        AS max_price,
    COUNT(DISTINCT v.CustID)                    AS unique_customers
FROM tbl_service_visit sv
JOIN tbl_visit   v ON sv.VisitID   = v.ID
LEFT JOIN tbl_service s ON sv.ServiceID = s.id
WHERE v.OrganizationId = %s
  AND v.RecDateTime >= %s
  AND v.RecDateTime <  %s
GROUP BY v.OrganizationId, sv.ServiceID, s.Name,
         DATE_FORMAT(v.RecDateTime, '%Y-%m-01')
""".strip()


class ServicesExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        return await self.fetch_all(_SQL, (org_id, period_start, end_excl))
