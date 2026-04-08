"""Loader for wh_payment_breakdown."""
from __future__ import annotations

from etl.base import BaseLoader


class PaymentsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_payment_breakdown (
            business_id, location_id, period_start, period_end,
            cash_amount, cash_count, card_amount, card_count,
            gift_card_amount, gift_card_count, other_amount, other_count,
            total_amount, total_count, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now()
        )
        ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
            period_end        = EXCLUDED.period_end,
            cash_amount       = EXCLUDED.cash_amount,
            cash_count        = EXCLUDED.cash_count,
            card_amount       = EXCLUDED.card_amount,
            card_count        = EXCLUDED.card_count,
            gift_card_amount  = EXCLUDED.gift_card_amount,
            gift_card_count   = EXCLUDED.gift_card_count,
            other_amount      = EXCLUDED.other_amount,
            other_count       = EXCLUDED.other_count,
            total_amount      = EXCLUDED.total_amount,
            total_count       = EXCLUDED.total_count,
            updated_at        = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["period_start"],
            row["period_end"],
            row["cash_amount"],
            row["cash_count"],
            row["card_amount"],
            row["card_count"],
            row["gift_card_amount"],
            row["gift_card_count"],
            row["other_amount"],
            row["other_count"],
            row["total_amount"],
            row["total_count"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
