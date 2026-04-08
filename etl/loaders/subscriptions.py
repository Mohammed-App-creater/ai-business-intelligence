"""Loader for wh_subscription_revenue."""
from __future__ import annotations

from etl.base import BaseLoader


class SubscriptionsLoader(BaseLoader):
    _SQL = """
        INSERT INTO wh_subscription_revenue (
            business_id, location_id, period_start, period_end,
            active_subscriptions, new_subscriptions, cancelled_subscriptions,
            gross_subscription_revenue, net_subscription_revenue, avg_subscription_value,
            updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()
        )
        ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
            period_end                 = EXCLUDED.period_end,
            active_subscriptions       = EXCLUDED.active_subscriptions,
            new_subscriptions          = EXCLUDED.new_subscriptions,
            cancelled_subscriptions    = EXCLUDED.cancelled_subscriptions,
            gross_subscription_revenue = EXCLUDED.gross_subscription_revenue,
            net_subscription_revenue   = EXCLUDED.net_subscription_revenue,
            avg_subscription_value     = EXCLUDED.avg_subscription_value,
            updated_at                 = now()
    """.strip()

    @staticmethod
    def _param_fn(row: dict) -> tuple:
        return (
            row["business_id"],
            row["location_id"],
            row["period_start"],
            row["period_end"],
            row["active_subscriptions"],
            row["new_subscriptions"],
            row["cancelled_subscriptions"],
            row["gross_subscription_revenue"],
            row["net_subscription_revenue"],
            row["avg_subscription_value"],
        )

    async def load(self, rows: list[dict]) -> tuple[int, int]:
        return await self.upsert_many(self._SQL, rows, self._param_fn)
