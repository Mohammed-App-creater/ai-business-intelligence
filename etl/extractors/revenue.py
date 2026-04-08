"""Monthly revenue extractor — tbl_visit."""
from __future__ import annotations

from datetime import date

from scripts.etl.base import BaseExtractor
from scripts.etl.extractors._util import period_end_exclusive

_SQL_PER_LOCATION = """
SELECT
    OrganizationId                                      AS business_id,
    LocationID                                          AS location_id,
    DATE_FORMAT(RecDateTime, '%Y-%m-01')                AS period_start,
    LAST_DAY(RecDateTime)                               AS period_end,
    SUM(Payment)                                        AS total_revenue,
    SUM(Tips)                                           AS total_tips,
    SUM(COALESCE(Tax, 0))                               AS total_tax,
    SUM(COALESCE(Discount, 0))                          AS total_discounts,
    SUM(GCAmount)                                       AS total_gc_amount,
    SUM(TotalPay)                                       AS gross_revenue,
    COUNT(*)                                            AS visit_count,
    SUM(CASE WHEN PaymentStatus = 1 THEN 1 ELSE 0 END) AS successful_visit_count,
    SUM(CASE WHEN PaymentStatus = 4 THEN 1 ELSE 0 END) AS refunded_visit_count,
    SUM(CASE WHEN PaymentStatus = 5 THEN 1 ELSE 0 END) AS cancelled_visit_count,
    SUM(CASE WHEN PaymentType = 'Cash'
             THEN TotalPay ELSE 0 END)                  AS cash_revenue,
    SUM(CASE WHEN PaymentType IN ('Credit', 'Check')
             THEN TotalPay ELSE 0 END)                  AS card_revenue,
    SUM(CASE WHEN PaymentType NOT IN ('Cash','Credit','Check','GiftCard')
             THEN TotalPay ELSE 0 END)                  AS other_revenue
FROM tbl_visit
WHERE OrganizationId = %s
  AND RecDateTime >= %s
  AND RecDateTime <  %s
GROUP BY OrganizationId, LocationID, DATE_FORMAT(RecDateTime, '%Y-%m-01')
""".strip()

_SQL_ROLLUP = """
SELECT
    OrganizationId                                      AS business_id,
    0                                                   AS location_id,
    DATE_FORMAT(RecDateTime, '%Y-%m-01')                AS period_start,
    LAST_DAY(RecDateTime)                               AS period_end,
    SUM(Payment)                                        AS total_revenue,
    SUM(Tips)                                           AS total_tips,
    SUM(COALESCE(Tax, 0))                               AS total_tax,
    SUM(COALESCE(Discount, 0))                          AS total_discounts,
    SUM(GCAmount)                                       AS total_gc_amount,
    SUM(TotalPay)                                       AS gross_revenue,
    COUNT(*)                                            AS visit_count,
    SUM(CASE WHEN PaymentStatus = 1 THEN 1 ELSE 0 END) AS successful_visit_count,
    SUM(CASE WHEN PaymentStatus = 4 THEN 1 ELSE 0 END) AS refunded_visit_count,
    SUM(CASE WHEN PaymentStatus = 5 THEN 1 ELSE 0 END) AS cancelled_visit_count,
    SUM(CASE WHEN PaymentType = 'Cash'
             THEN TotalPay ELSE 0 END)                  AS cash_revenue,
    SUM(CASE WHEN PaymentType IN ('Credit', 'Check')
             THEN TotalPay ELSE 0 END)                  AS card_revenue,
    SUM(CASE WHEN PaymentType NOT IN ('Cash','Credit','Check','GiftCard')
             THEN TotalPay ELSE 0 END)                  AS other_revenue
FROM tbl_visit
WHERE OrganizationId = %s
  AND RecDateTime >= %s
  AND RecDateTime <  %s
GROUP BY OrganizationId, DATE_FORMAT(RecDateTime, '%Y-%m-01')
""".strip()


class RevenueExtractor(BaseExtractor):
    async def extract(
        self,
        org_id: int,
        period_start: date,
        period_end: date,
    ) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        params = (org_id, period_start, end_excl)
        per_loc = await self.fetch_all(_SQL_PER_LOCATION, params)
        rollup = await self.fetch_all(_SQL_ROLLUP, params)
        return list(per_loc) + list(rollup)
