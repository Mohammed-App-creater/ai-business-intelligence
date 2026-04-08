"""Loader for wh_expense_summary."""
from __future__ import annotations

from etl.base import BaseLoader


class ExpensesLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_expense_summary (
            business_id, location_id, category_id, category_name,
            period_start, period_end, total_amount, expense_count, avg_expense,
            updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,now()
        )
        ON CONFLICT (business_id, location_id, category_id, period_start) DO UPDATE SET
            category_name = EXCLUDED.category_name,
            period_end    = EXCLUDED.period_end,
            total_amount  = EXCLUDED.total_amount,
            expense_count = EXCLUDED.expense_count,
            avg_expense   = EXCLUDED.avg_expense,
            updated_at    = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["category_id"],
            row["category_name"],
            row["period_start"],
            row["period_end"],
            row["total_amount"],
            row["expense_count"],
            row["avg_expense"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
