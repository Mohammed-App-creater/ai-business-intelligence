"""Expense summary extractor."""
from __future__ import annotations

from datetime import date

from etl.base import BaseExtractor
from etl.extractors._util import period_end_exclusive

_SQL = """
SELECT
    e.OrganizationId                            AS business_id,
    e.LocationID                                AS location_id,
    e.CategoryID                                AS category_id,
    COALESCE(ec.Category, 'Uncategorised')      AS category_name,
    DATE_FORMAT(e.RecDateTime, '%Y-%m-01')    AS period_start,
    LAST_DAY(e.RecDateTime)                     AS period_end,
    SUM(e.Amount)                               AS total_amount,
    COUNT(*)                                    AS expense_count
FROM tbl_expense e
LEFT JOIN tbl_expense_category ec ON e.CategoryID = ec.id
WHERE e.OrganizationId = %s
  AND e.RecDateTime >= %s
  AND e.RecDateTime <  %s
  AND e.isDeleted = 0
GROUP BY e.OrganizationId, e.LocationID, e.CategoryID, ec.Category,
         DATE_FORMAT(e.RecDateTime, '%Y-%m-01')
""".strip()


class ExpensesExtractor(BaseExtractor):
    async def extract(self, org_id: int, period_start: date, period_end: date) -> list[dict]:
        end_excl = period_end_exclusive(period_end)
        return await self.fetch_all(_SQL, (org_id, period_start, end_excl))
