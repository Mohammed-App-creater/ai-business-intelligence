"""Subscription rows extractor — active + newly created in window."""
from __future__ import annotations

from datetime import date

from etl.base import BaseExtractor
from etl.extractors._util import period_end_exclusive

_SQL = """
SELECT
    OrgId                   AS business_id,
    LocId                   AS location_id,
    DATE_FORMAT(%s, '%Y-%m-01')   AS period_start,
    LAST_DAY(%s)                    AS period_end,
    CustId                  AS customer_id,
    Amount                  AS amount,
    Discount                AS discount,
    Active                  AS is_active,
    DATE(SubCreateDate)     AS sub_create_date
FROM tbl_custsubscription
WHERE OrgId = %s
  AND (
    (Active = 1 AND SubExecutionDate >= %s AND SubExecutionDate < %s)
    OR
    (SubCreateDate >= %s AND SubCreateDate < %s)
  )
""".strip()


class SubscriptionsExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        params = (
            period_start,
            period_start,
            org_id,
            period_start,
            end_excl,
            period_start,
            end_excl,
        )
        return await self.fetch_all(_SQL, params)
