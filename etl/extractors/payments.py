"""Payment breakdown extractor — per payment_type rows."""
from __future__ import annotations

from datetime import date

from scripts.etl.base import BaseExtractor
from scripts.etl.extractors._util import period_end_exclusive

_SQL_PER_LOC = """
SELECT
    OrganizationId                              AS business_id,
    LocationID                                  AS location_id,
    DATE_FORMAT(RecDateTime, '%Y-%m-01')      AS period_start,
    LAST_DAY(RecDateTime)                       AS period_end,
    PaymentType                                 AS payment_type,
    SUM(TotalPay)                               AS amount,
    COUNT(*)                                    AS count
FROM tbl_visit
WHERE OrganizationId = %s
  AND RecDateTime >= %s
  AND RecDateTime <  %s
  AND PaymentStatus = 1
GROUP BY OrganizationId, LocationID,
         DATE_FORMAT(RecDateTime, '%Y-%m-01'), PaymentType
""".strip()

_SQL_ROLLUP = """
SELECT
    OrganizationId                              AS business_id,
    0                                           AS location_id,
    DATE_FORMAT(RecDateTime, '%Y-%m-01')      AS period_start,
    LAST_DAY(RecDateTime)                       AS period_end,
    PaymentType                                 AS payment_type,
    SUM(TotalPay)                               AS amount,
    COUNT(*)                                    AS count
FROM tbl_visit
WHERE OrganizationId = %s
  AND RecDateTime >= %s
  AND RecDateTime <  %s
  AND PaymentStatus = 1
GROUP BY OrganizationId,
         DATE_FORMAT(RecDateTime, '%Y-%m-01'), PaymentType
""".strip()


class PaymentsExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        params = (org_id, period_start, end_excl)
        per_loc = await self.fetch_all(_SQL_PER_LOC, params)
        rollup = await self.fetch_all(_SQL_ROLLUP, params)
        return list(per_loc) + list(rollup)
