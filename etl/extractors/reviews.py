"""Reviews extractor — returns three row lists for transform_reviews."""
from __future__ import annotations

from datetime import date

from etl.base import BaseExtractor
from etl.extractors._util import period_end_exclusive

_SQL_EMP = """
SELECT
    e.OrganizationId                            AS business_id,
    DATE_FORMAT(r.created_at, '%Y-%m-01')    AS period_start,
    LAST_DAY(r.created_at)                      AS period_end,
    COUNT(*)                                    AS emp_review_count,
    AVG(r.rating)                               AS emp_avg_rating
FROM tbl_emp_reviews r
JOIN tbl_emp e ON r.employee_id = e.id
WHERE e.OrganizationId = %s
  AND r.created_at >= %s
  AND r.created_at <  %s
  AND r.rating IS NOT NULL
GROUP BY e.OrganizationId, DATE_FORMAT(r.created_at, '%Y-%m-01')
""".strip()

_SQL_VISIT = """
SELECT
    OrganizationId                              AS business_id,
    DATE_FORMAT(CreatedAt, '%Y-%m-01')        AS period_start,
    LAST_DAY(CreatedAt)                         AS period_end,
    COUNT(*)                                    AS visit_review_count,
    AVG(Rating)                                 AS visit_avg_rating
FROM tbl_visit_review
WHERE OrganizationId = %s
  AND CreatedAt >= %s
  AND CreatedAt <  %s
GROUP BY OrganizationId, DATE_FORMAT(CreatedAt, '%Y-%m-01')
""".strip()

_SQL_GOOGLE = """
SELECT
    organization_id                             AS business_id,
    DATE_FORMAT(review_time, '%Y-%m-01')      AS period_start,
    LAST_DAY(review_time)                       AS period_end,
    COUNT(*)                                    AS google_review_count,
    AVG(rating)                                 AS google_avg_rating,
    SUM(CASE WHEN is_bad_review = 1 THEN 1 ELSE 0 END) AS google_bad_review_count
FROM tbl_google_review
WHERE organization_id = %s
  AND review_time >= %s
  AND review_time <  %s
  AND rating IS NOT NULL
GROUP BY organization_id, DATE_FORMAT(review_time, '%Y-%m-01')
""".strip()


class ReviewsExtractor(BaseExtractor):
    async def extract(
        self,
        org_id: int,
        period_start: date,
        period_end: date,
    ) -> tuple[list[dict], list[dict], list[dict]]:
        end_excl = period_end_exclusive(period_end)
        params = (org_id, period_start, end_excl)
        emp_rows = await self.fetch_all(_SQL_EMP, params)
        visit_rows = await self.fetch_all(_SQL_VISIT, params)
        google_rows = await self.fetch_all(_SQL_GOOGLE, params)
        return list(emp_rows), list(visit_rows), list(google_rows)
