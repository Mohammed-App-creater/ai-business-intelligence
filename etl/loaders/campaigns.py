"""Loader for wh_campaign_performance."""
from __future__ import annotations

from etl.base import BaseLoader


class CampaignsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_campaign_performance (
            business_id, campaign_id, campaign_name, execution_date, is_recurring,
            total_sent, successful_sent, failed_count, opened_count, clicked_count,
            open_rate, click_rate, fail_rate, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now()
        )
        ON CONFLICT (business_id, campaign_id, execution_date) DO UPDATE SET
            campaign_name   = EXCLUDED.campaign_name,
            is_recurring    = EXCLUDED.is_recurring,
            total_sent      = EXCLUDED.total_sent,
            successful_sent = EXCLUDED.successful_sent,
            failed_count    = EXCLUDED.failed_count,
            opened_count    = EXCLUDED.opened_count,
            clicked_count   = EXCLUDED.clicked_count,
            open_rate       = EXCLUDED.open_rate,
            click_rate      = EXCLUDED.click_rate,
            fail_rate       = EXCLUDED.fail_rate,
            updated_at      = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["campaign_id"],
            row["campaign_name"],
            row["execution_date"],
            row["is_recurring"],
            row["total_sent"],
            row["successful_sent"],
            row["failed_count"],
            row["opened_count"],
            row["clicked_count"],
            row["open_rate"],
            row["click_rate"],
            row["fail_rate"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
