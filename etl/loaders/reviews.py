"""Loader for wh_review_summary."""
from __future__ import annotations

from scripts.etl.base import BaseLoader


class ReviewsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_review_summary (
            business_id, period_start, period_end,
            emp_review_count, emp_avg_rating,
            visit_review_count, visit_avg_rating,
            google_review_count, google_avg_rating, google_bad_review_count,
            total_review_count, overall_avg_rating, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now()
        )
        ON CONFLICT (business_id, period_start) DO UPDATE SET
            period_end               = EXCLUDED.period_end,
            emp_review_count         = EXCLUDED.emp_review_count,
            emp_avg_rating           = EXCLUDED.emp_avg_rating,
            visit_review_count       = EXCLUDED.visit_review_count,
            visit_avg_rating         = EXCLUDED.visit_avg_rating,
            google_review_count      = EXCLUDED.google_review_count,
            google_avg_rating        = EXCLUDED.google_avg_rating,
            google_bad_review_count  = EXCLUDED.google_bad_review_count,
            total_review_count       = EXCLUDED.total_review_count,
            overall_avg_rating       = EXCLUDED.overall_avg_rating,
            updated_at               = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["period_start"],
            row["period_end"],
            row["emp_review_count"],
            row["emp_avg_rating"],
            row["visit_review_count"],
            row["visit_avg_rating"],
            row["google_review_count"],
            row["google_avg_rating"],
            row["google_bad_review_count"],
            row["total_review_count"],
            row["overall_avg_rating"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
