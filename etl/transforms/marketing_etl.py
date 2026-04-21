"""
etl/transforms/marketing_etl.py
================================
Marketing domain ETL extractor.

Pulls all 3 marketing data slices from the analytics backend, writes them
to the warehouse (wh_mrk_* tables), and returns structured in-memory dicts
for immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  MarketingExtractor.run()
        ↓  _write_to_warehouse()
    wh_mrk_campaign_summary
    wh_mrk_channel_monthly
    wh_mrk_promo_attribution_monthly
        ↓  returned to doc generator → pgvector

Cadence: daily full re-aggregate + hourly incremental for last 14 days
(identical to all prior signed-off domains).

Usage (with warehouse write — production):
    extractor = MarketingExtractor(client=analytics_client, wh_pool=wh_pool)
    results = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — test/debug):
    extractor = MarketingExtractor(client=analytics_client)
    results = await extractor.run(business_id=42, start_date=..., end_date=...)

Returns: dict with three keys:
    {
        "campaign_summary":   [...],   # list[dict] — one row per execution
        "channel_monthly":    [...],   # list[dict] — one row per period
        "promo_attribution":  [...],   # list[dict] — one row per (campaign,period,location)
    }
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class MarketingExtractor:
    """
    Pulls and transforms all marketing data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg Pool — when provided, writes extracted
              rows to the warehouse before returning. When None, the
              warehouse write is skipped (useful in tests).
    """

    DOMAIN = "marketing"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client  = client
        self.wh_pool = wh_pool

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Fetch all 3 marketing slices, write to warehouse, return in-memory rows.

        Returns
        -------
        dict with keys: campaign_summary, channel_monthly, promo_attribution
        Each value is a list[dict] ready for the doc generator.
        """
        logger.info(
            "MarketingExtractor: business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch all 3 slices in parallel ────────────────────────────────
        campaign_raw, channel_raw, promo_raw = await asyncio.gather(
            self.client.get_marketing_campaign_summary(
                business_id, start_date, end_date,
            ),
            self.client.get_marketing_channel_monthly(
                business_id, start_date, end_date,
            ),
            self.client.get_marketing_promo_attribution_monthly(
                business_id, start_date, end_date,
            ),
        )

        # ── 2. Transform each slice ──────────────────────────────────────────
        campaign_rows = self._transform_campaign_summary(business_id, campaign_raw)
        channel_rows  = self._transform_channel_monthly(business_id, channel_raw)
        promo_rows    = self._transform_promo_attribution(business_id, promo_raw)

        # ── 3. Compute net-unsub deltas (depends on sorted order) ────────────
        # The mock server / production API already computes these, but we
        # recompute as a defensive measure in case the backend ever returns
        # partial data and we need to derive from what's in hand.
        self._compute_net_unsub_deltas(channel_rows)

        # ── 4. Write to warehouse (if pool provided) ─────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                business_id, campaign_rows, channel_rows, promo_rows,
            )
        else:
            logger.debug(
                "MarketingExtractor: wh_pool not provided — skipping warehouse write"
            )

        logger.info(
            "MarketingExtractor: business_id=%s produced %d campaign + %d channel + %d promo rows",
            business_id, len(campaign_rows), len(channel_rows), len(promo_rows),
        )

        return {
            "campaign_summary":  campaign_rows,
            "channel_monthly":   channel_rows,
            "promo_attribution": promo_rows,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Transforms
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_campaign_summary(
        self, business_id: int, raw: list[dict],
    ) -> list[dict]:
        """Stamp business_id on each row; normalize NULL-safe fields."""
        out = []
        for r in raw:
            out.append({
                "business_id":            business_id,
                "campaign_id":            r.get("campaign_id"),
                "execution_date":         r.get("execution_date"),
                "period":                 r.get("period"),
                "campaign_name":          r.get("campaign_name") or "",
                "campaign_status":        r.get("campaign_status") or "unknown",
                "is_active":              int(r.get("is_active") or 0),
                "is_recurring":           int(r.get("is_recurring") or 0),
                "channel":                r.get("channel") or "unknown",
                "channel_code":           int(r.get("channel_code") or 0),
                "template_format_name":   r.get("template_format_name"),
                "audience_size":          r.get("audience_size"),
                "promo_code_string":      r.get("promo_code_string"),
                "campaign_start":         r.get("campaign_start"),
                "campaign_expiration":    r.get("campaign_expiration"),
                "total_sent":             int(r.get("total_sent") or 0),
                "delivered":              int(r.get("delivered") or 0),
                "failed":                 int(r.get("failed") or 0),
                "opened":                 int(r.get("opened") or 0),
                "clicked":                int(r.get("clicked") or 0),
                "delivery_rate_pct":      r.get("delivery_rate_pct"),
                "open_rate_pct":          r.get("open_rate_pct"),
                "click_rate_pct":         r.get("click_rate_pct"),
                "ctr_engagement_pct":     r.get("ctr_engagement_pct"),
                "is_expired_but_active":  int(r.get("is_expired_but_active") or 0),
                "rank_open_in_period":    r.get("rank_open_in_period"),
                "rank_click_in_period":   r.get("rank_click_in_period"),
                "rank_reach_in_period":   r.get("rank_reach_in_period"),
            })
        return out

    def _transform_channel_monthly(
        self, business_id: int, raw: list[dict],
    ) -> list[dict]:
        """Stamp business_id on each row."""
        out = []
        for r in raw:
            out.append({
                "business_id":                business_id,
                "period":                     r.get("period"),
                "emails_sent":                int(r.get("emails_sent") or 0),
                "sms_sent":                   int(r.get("sms_sent") or 0),
                "prev_emails_sent":           r.get("prev_emails_sent"),
                "prev_sms_sent":              r.get("prev_sms_sent"),
                "emails_mom_pct":             r.get("emails_mom_pct"),
                "sms_mom_pct":                r.get("sms_mom_pct"),
                "email_campaigns_run":        r.get("email_campaigns_run"),
                "email_open_rate_pct":        r.get("email_open_rate_pct"),
                "email_click_rate_pct":       r.get("email_click_rate_pct"),
                "sms_campaigns_run":          r.get("sms_campaigns_run"),
                "sms_open_rate_pct":          r.get("sms_open_rate_pct"),   # expect NULL
                "sms_click_rate_pct":         r.get("sms_click_rate_pct"),
                "email_unsubscribed_count":   r.get("email_unsubscribed_count"),
                "sms_unsubscribed_count":     r.get("sms_unsubscribed_count"),
                "total_contacts":             r.get("total_contacts"),
                "email_contactable":          r.get("email_contactable"),
                "sms_contactable":            r.get("sms_contactable"),
                "email_net_unsub_delta":      r.get("email_net_unsub_delta"),
                "sms_net_unsub_delta":        r.get("sms_net_unsub_delta"),
                "email_contactable_mom_pct":  r.get("email_contactable_mom_pct"),
            })
        return out

    def _transform_promo_attribution(
        self, business_id: int, raw: list[dict],
    ) -> list[dict]:
        """Stamp business_id on each row."""
        out = []
        for r in raw:
            out.append({
                "business_id":                 business_id,
                "campaign_id":                 r.get("campaign_id"),
                "period":                      r.get("period"),
                "location_id":                 r.get("location_id"),
                "campaign_name":               r.get("campaign_name") or "",
                "promo_code_string":           r.get("promo_code_string") or "",
                "audience_size":               r.get("audience_size"),
                "redemptions":                 int(r.get("redemptions") or 0),
                "attributed_revenue":          float(r.get("attributed_revenue") or 0),
                "total_discount_given":        float(r.get("total_discount_given") or 0),
                "net_revenue_after_discount":  float(r.get("net_revenue_after_discount") or 0),
                "revenue_per_send":            r.get("revenue_per_send"),
                "conversion_rate_pct":         r.get("conversion_rate_pct"),
                "rank_in_period":              r.get("rank_in_period"),
                "rank_in_location_period":     r.get("rank_in_location_period"),
            })
        return out

    # ─────────────────────────────────────────────────────────────────────────
    # Derived-field computation (defensive — backend already does this)
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_net_unsub_deltas(self, channel_rows: list[dict]) -> None:
        """
        Compute email_net_unsub_delta / sms_net_unsub_delta from the series
        in-hand. Only overwrites NULL values — if the backend computed them
        correctly (expected path), they're preserved.

        Channel rows come back from the API sorted period DESC. To compute
        "current - previous", we sort ascending, walk through, and stamp deltas.
        """
        if not channel_rows:
            return

        asc = sorted(channel_rows, key=lambda r: r["period"] or "")

        prev_email_unsub = None
        prev_sms_unsub   = None
        for row in asc:
            if row["email_net_unsub_delta"] is None and prev_email_unsub is not None:
                if row["email_unsubscribed_count"] is not None:
                    row["email_net_unsub_delta"] = (
                        row["email_unsubscribed_count"] - prev_email_unsub
                    )
            if row["sms_net_unsub_delta"] is None and prev_sms_unsub is not None:
                if row["sms_unsubscribed_count"] is not None:
                    row["sms_net_unsub_delta"] = (
                        row["sms_unsubscribed_count"] - prev_sms_unsub
                    )

            if row["email_unsubscribed_count"] is not None:
                prev_email_unsub = row["email_unsubscribed_count"]
            if row["sms_unsubscribed_count"] is not None:
                prev_sms_unsub = row["sms_unsubscribed_count"]

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse writes — idempotent ON CONFLICT DO UPDATE
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        campaign_rows: list[dict],
        channel_rows: list[dict],
        promo_rows: list[dict],
    ) -> None:
        """Upsert all three slices into wh_mrk_* tables."""
        async with self.wh_pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_campaign_summary(conn, campaign_rows)
                await self._upsert_channel_monthly(conn, channel_rows)
                await self._upsert_promo_attribution(conn, promo_rows)

        logger.info(
            "MarketingExtractor: warehouse write complete — "
            "campaign=%d channel=%d promo=%d",
            len(campaign_rows), len(channel_rows), len(promo_rows),
        )

    async def _upsert_campaign_summary(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_mrk_campaign_summary (
                business_id, campaign_id, execution_date, period,
                campaign_name, campaign_status, is_active, is_recurring,
                channel, channel_code, template_format_name, audience_size,
                promo_code_string, campaign_start, campaign_expiration,
                total_sent, delivered, failed, opened, clicked,
                delivery_rate_pct, open_rate_pct, click_rate_pct, ctr_engagement_pct,
                is_expired_but_active,
                rank_open_in_period, rank_click_in_period, rank_reach_in_period,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, NOW()
            )
            ON CONFLICT (business_id, campaign_id, COALESCE(execution_date, DATE '1900-01-01'))
            DO UPDATE SET
                period                = EXCLUDED.period,
                campaign_name         = EXCLUDED.campaign_name,
                campaign_status       = EXCLUDED.campaign_status,
                is_active             = EXCLUDED.is_active,
                is_recurring          = EXCLUDED.is_recurring,
                channel               = EXCLUDED.channel,
                channel_code          = EXCLUDED.channel_code,
                template_format_name  = EXCLUDED.template_format_name,
                audience_size         = EXCLUDED.audience_size,
                promo_code_string     = EXCLUDED.promo_code_string,
                campaign_start        = EXCLUDED.campaign_start,
                campaign_expiration   = EXCLUDED.campaign_expiration,
                total_sent            = EXCLUDED.total_sent,
                delivered             = EXCLUDED.delivered,
                failed                = EXCLUDED.failed,
                opened                = EXCLUDED.opened,
                clicked               = EXCLUDED.clicked,
                delivery_rate_pct     = EXCLUDED.delivery_rate_pct,
                open_rate_pct         = EXCLUDED.open_rate_pct,
                click_rate_pct        = EXCLUDED.click_rate_pct,
                ctr_engagement_pct    = EXCLUDED.ctr_engagement_pct,
                is_expired_but_active = EXCLUDED.is_expired_but_active,
                rank_open_in_period   = EXCLUDED.rank_open_in_period,
                rank_click_in_period  = EXCLUDED.rank_click_in_period,
                rank_reach_in_period  = EXCLUDED.rank_reach_in_period,
                updated_at            = NOW();
        """
        await conn.executemany(sql, [
            (
                r["business_id"], r["campaign_id"],
                _to_date(r["execution_date"]), _to_date(r["period"]),
                r["campaign_name"], r["campaign_status"],
                r["is_active"], r["is_recurring"],
                r["channel"], r["channel_code"],
                r["template_format_name"], r["audience_size"],
                r["promo_code_string"],
                _to_date(r["campaign_start"]), _to_date(r["campaign_expiration"]),
                r["total_sent"], r["delivered"], r["failed"],
                r["opened"], r["clicked"],
                r["delivery_rate_pct"], r["open_rate_pct"],
                r["click_rate_pct"], r["ctr_engagement_pct"],
                r["is_expired_but_active"],
                r["rank_open_in_period"], r["rank_click_in_period"],
                r["rank_reach_in_period"],
            )
            for r in rows
        ])

    async def _upsert_channel_monthly(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_mrk_channel_monthly (
                business_id, period,
                emails_sent, sms_sent,
                prev_emails_sent, prev_sms_sent,
                emails_mom_pct, sms_mom_pct,
                email_campaigns_run, email_open_rate_pct, email_click_rate_pct,
                sms_campaigns_run, sms_open_rate_pct, sms_click_rate_pct,
                email_unsubscribed_count, sms_unsubscribed_count,
                total_contacts, email_contactable, sms_contactable,
                email_net_unsub_delta, sms_net_unsub_delta,
                email_contactable_mom_pct,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, NOW()
            )
            ON CONFLICT (business_id, period)
            DO UPDATE SET
                emails_sent                = EXCLUDED.emails_sent,
                sms_sent                   = EXCLUDED.sms_sent,
                prev_emails_sent           = EXCLUDED.prev_emails_sent,
                prev_sms_sent              = EXCLUDED.prev_sms_sent,
                emails_mom_pct             = EXCLUDED.emails_mom_pct,
                sms_mom_pct                = EXCLUDED.sms_mom_pct,
                email_campaigns_run        = EXCLUDED.email_campaigns_run,
                email_open_rate_pct        = EXCLUDED.email_open_rate_pct,
                email_click_rate_pct       = EXCLUDED.email_click_rate_pct,
                sms_campaigns_run          = EXCLUDED.sms_campaigns_run,
                sms_open_rate_pct          = EXCLUDED.sms_open_rate_pct,
                sms_click_rate_pct         = EXCLUDED.sms_click_rate_pct,
                email_unsubscribed_count   = EXCLUDED.email_unsubscribed_count,
                sms_unsubscribed_count     = EXCLUDED.sms_unsubscribed_count,
                total_contacts             = EXCLUDED.total_contacts,
                email_contactable          = EXCLUDED.email_contactable,
                sms_contactable            = EXCLUDED.sms_contactable,
                email_net_unsub_delta      = EXCLUDED.email_net_unsub_delta,
                sms_net_unsub_delta        = EXCLUDED.sms_net_unsub_delta,
                email_contactable_mom_pct  = EXCLUDED.email_contactable_mom_pct,
                updated_at                 = NOW();
        """
        await conn.executemany(sql, [
            (
                r["business_id"], _to_date(r["period"]),
                r["emails_sent"], r["sms_sent"],
                r["prev_emails_sent"], r["prev_sms_sent"],
                r["emails_mom_pct"], r["sms_mom_pct"],
                r["email_campaigns_run"], r["email_open_rate_pct"], r["email_click_rate_pct"],
                r["sms_campaigns_run"], r["sms_open_rate_pct"], r["sms_click_rate_pct"],
                r["email_unsubscribed_count"], r["sms_unsubscribed_count"],
                r["total_contacts"], r["email_contactable"], r["sms_contactable"],
                r["email_net_unsub_delta"], r["sms_net_unsub_delta"],
                r["email_contactable_mom_pct"],
            )
            for r in rows
        ])

    async def _upsert_promo_attribution(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_mrk_promo_attribution_monthly (
                business_id, campaign_id, period, location_id,
                campaign_name, promo_code_string, audience_size,
                redemptions, attributed_revenue,
                total_discount_given, net_revenue_after_discount,
                revenue_per_send, conversion_rate_pct,
                rank_in_period, rank_in_location_period,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, NOW()
            )
            ON CONFLICT (business_id, campaign_id, period, location_id)
            DO UPDATE SET
                campaign_name              = EXCLUDED.campaign_name,
                promo_code_string          = EXCLUDED.promo_code_string,
                audience_size              = EXCLUDED.audience_size,
                redemptions                = EXCLUDED.redemptions,
                attributed_revenue         = EXCLUDED.attributed_revenue,
                total_discount_given       = EXCLUDED.total_discount_given,
                net_revenue_after_discount = EXCLUDED.net_revenue_after_discount,
                revenue_per_send           = EXCLUDED.revenue_per_send,
                conversion_rate_pct        = EXCLUDED.conversion_rate_pct,
                rank_in_period             = EXCLUDED.rank_in_period,
                rank_in_location_period    = EXCLUDED.rank_in_location_period,
                updated_at                 = NOW();
        """
        await conn.executemany(sql, [
            (
                r["business_id"], r["campaign_id"],
                _to_date(r["period"]), r["location_id"],
                r["campaign_name"], r["promo_code_string"],
                r["audience_size"],
                r["redemptions"], r["attributed_revenue"],
                r["total_discount_given"], r["net_revenue_after_discount"],
                r["revenue_per_send"], r["conversion_rate_pct"],
                r["rank_in_period"], r["rank_in_location_period"],
            )
            for r in rows
        ])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_date(value):
    """Convert an ISO date string to a date object; pass-through None/date."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    # Accept YYYY-MM-DD or YYYY-MM-DD...T... (strip to first 10 chars)
    return date.fromisoformat(str(value)[:10])