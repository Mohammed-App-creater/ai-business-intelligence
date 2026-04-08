"""
LEO AI BI — Revenue ETL Extractor
Revenue Domain — Step 4: ETL Wire-Up

Pulls all 6 revenue data slices from the analytics backend,
computes any warehouse-layer fields, and returns structured
documents ready for embedding and storage in pgvector.

Usage:
    extractor = RevenueExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
    # docs → list of dicts, each becomes one pgvector row
"""

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import logging

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class RevenueExtractor:
    """
    Pulls and transforms all revenue data for one tenant.
    Output: a list of structured embedding documents.
    """

    DOMAIN = "revenue"

    def __init__(self, client: AnalyticsClient):
        self.client = client

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Main entry point. Fetches all 6 slices and returns
        warehouse-ready documents for this tenant.
        """
        logger.info(
            "Revenue ETL start — business_id=%s %s → %s",
            business_id,
            start_date,
            end_date,
        )

        # Fetch all 6 slices in sequence
        # (parallelise with asyncio.gather once stable)
        monthly     = await self.client.get_revenue_monthly_summary(business_id, start_date, end_date)
        pay_types   = await self.client.get_revenue_payment_types(business_id, start_date, end_date)
        by_staff    = await self.client.get_revenue_by_staff(business_id, start_date, end_date)
        by_location = await self.client.get_revenue_by_location(business_id, start_date, end_date)
        promos      = await self.client.get_revenue_promo_impact(business_id, start_date, end_date)
        failed      = await self.client.get_revenue_failed_refunds(business_id, start_date, end_date)

        docs = []

        # ── 1. Monthly summary documents (one doc per period) ────────────────
        for row in monthly:
            doc = self._build_monthly_doc(business_id, row, monthly)
            docs.append(doc)

        # ── 2. Payment type summary (one aggregate doc) ──────────────────────
        if pay_types:
            docs.append(self._build_payment_type_doc(business_id, pay_types, start_date, end_date))

        # ── 3. Staff revenue (one doc per staff member) ──────────────────────
        for row in by_staff:
            docs.append(self._build_staff_doc(business_id, row, start_date, end_date))

        # ── 4. Location revenue (one doc per location per period) ────────────
        for row in by_location:
            docs.append(self._build_location_doc(business_id, row))

        # ── 5. Promo impact (one aggregate doc) ──────────────────────────────
        if promos:
            docs.append(self._build_promo_doc(business_id, promos, start_date, end_date))

        # ── 6. Failed / refunded visits (one aggregate doc) ──────────────────
        if failed:
            docs.append(self._build_failed_doc(business_id, failed, start_date, end_date))

        logger.info(
            "Revenue ETL complete — business_id=%s, %d docs produced",
            business_id,
            len(docs),
        )
        return docs

    # ── Document builders ────────────────────────────────────────────────────

    def _build_monthly_doc(self, business_id: int, row: dict, all_rows: list[dict]) -> dict:
        """One document per period — the core revenue time-series row."""
        trend_slope = self._compute_trend_slope(all_rows)
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "monthly_summary",
            "period": row["period"],
            # Core metrics
            "visit_count": row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "total_tips": row.get("total_tips", 0.0),
            "total_tax": row.get("total_tax", 0.0),
            "total_collected": row.get("total_collected", 0.0),
            "total_discounts": row.get("total_discounts", 0.0),
            "gc_redemptions": row.get("gc_redemptions", 0.0),
            "avg_ticket": row.get("avg_ticket", 0.0),
            # Computed
            "mom_growth_pct": row.get("mom_growth_pct"),  # nullable for first period
            "trend_slope": trend_slope,
            "trend_direction": "up" if trend_slope > 0 else ("down" if trend_slope < 0 else "flat"),
            # Edge-case counts
            "refund_count": row.get("refund_count", 0),
            "cancel_count": row.get("cancel_count", 0),
            # Embedding text — what the RAG system will search
            "text": self._monthly_text(row, trend_slope),
        }

    def _build_payment_type_doc(
        self, business_id: int, rows: list[dict], start_date: date, end_date: date
    ) -> dict:
        sorted_rows = sorted(rows, key=lambda r: r.get("revenue", 0), reverse=True)
        lines = [
            f"{r['payment_type']}: ${r['revenue']:,.2f} ({r['pct_of_total']}%)"
            for r in sorted_rows
        ]
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "payment_type_breakdown",
            "period": f"{start_date} to {end_date}",
            "breakdown": sorted_rows,
            "text": (
                f"Payment type breakdown from {start_date} to {end_date}: "
                + ", ".join(lines)
                + ". "
                + f"Top payment method: {sorted_rows[0]['payment_type']} "
                  f"at {sorted_rows[0]['pct_of_total']}% of revenue."
            ),
        }

    def _build_staff_doc(
        self, business_id: int, row: dict, start_date: date, end_date: date
    ) -> dict:
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "staff_revenue",
            "period": f"{start_date} to {end_date}",
            "emp_id": row.get("emp_id"),
            "staff_name": row.get("staff_name"),
            "visit_count": row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "tips_collected": row.get("tips_collected", 0.0),
            "avg_ticket": row.get("avg_ticket", 0.0),
            "revenue_rank": row.get("revenue_rank"),
            "text": (
                f"{row.get('staff_name')} generated ${row.get('service_revenue', 0):,.2f} "
                f"in service revenue from {start_date} to {end_date} "
                f"across {row.get('visit_count', 0)} visits "
                f"(avg ticket ${row.get('avg_ticket', 0):,.2f}, "
                f"tips ${row.get('tips_collected', 0):,.2f}). "
                f"Revenue rank: #{row.get('revenue_rank')}."
            ),
        }

    def _build_location_doc(self, business_id: int, row: dict) -> dict:
        mom = row.get("mom_growth_pct")
        mom_text = f", {mom:+.1f}% vs previous period" if mom is not None else ""
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "location_revenue",
            "period": row.get("period"),
            "location_id": row.get("location_id"),
            "location_name": row.get("location_name"),
            "visit_count": row.get("visit_count", 0),
            "service_revenue": row.get("service_revenue", 0.0),
            "total_tips": row.get("total_tips", 0.0),
            "avg_ticket": row.get("avg_ticket", 0.0),
            "total_discounts": row.get("total_discounts", 0.0),
            "gc_redemptions": row.get("gc_redemptions", 0.0),
            "pct_of_total_revenue": row.get("pct_of_total_revenue"),
            "mom_growth_pct": mom,
            "text": (
                f"{row.get('location_name')} location revenue for {row.get('period')}: "
                f"${row.get('service_revenue', 0):,.2f} "
                f"({row.get('pct_of_total_revenue', 0):.1f}% of total){mom_text}. "
                f"{row.get('visit_count', 0)} visits, avg ticket ${row.get('avg_ticket', 0):,.2f}."
            ),
        }

    def _build_promo_doc(
        self, business_id: int, rows: list[dict], start_date: date, end_date: date
    ) -> dict:
        total_discount = sum(r.get("total_discount_given", 0) for r in rows)
        total_uses = sum(r.get("times_used", 0) for r in rows)
        lines = [
            f"'{r['promo_code']}' used {r['times_used']}x = ${r['total_discount_given']:,.2f} off"
            for r in rows
        ]
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "promo_impact",
            "period": f"{start_date} to {end_date}",
            "total_discount_given": total_discount,
            "total_promo_uses": total_uses,
            "breakdown": rows,
            "text": (
                f"Promo code impact from {start_date} to {end_date}: "
                f"total discount given = ${total_discount:,.2f} across {total_uses} uses. "
                + " | ".join(lines) + "."
            ),
        }

    def _build_failed_doc(
        self, business_id: int, rows: list[dict], start_date: date, end_date: date
    ) -> dict:
        total_lost = sum(r.get("lost_revenue", 0) for r in rows)
        total_visits = sum(r.get("visit_count", 0) for r in rows)
        lines = [
            f"{r['status_label']}: {r['visit_count']} visits = ${r['lost_revenue']:,.2f}"
            for r in rows
        ]
        return {
            "tenant_id": business_id,
            "domain": self.DOMAIN,
            "doc_type": "failed_refunds",
            "period": f"{start_date} to {end_date}",
            "total_lost_revenue": total_lost,
            "total_affected_visits": total_visits,
            "breakdown": rows,
            "text": (
                f"Failed and refunded visits from {start_date} to {end_date}: "
                f"{total_visits} affected visits, ${total_lost:,.2f} in lost/reversed revenue. "
                + " | ".join(lines) + ". "
                f"Note: no-show cost (tbl_calendarevent) is a known gap — not included here."
            ),
        }

    # ── Computed column helpers ──────────────────────────────────────────────

    @staticmethod
    def _compute_trend_slope(rows: list[dict]) -> float:
        """
        Linear regression slope over service_revenue values in time order.
        Positive = growing, negative = shrinking, 0 = flat.
        Returns 0.0 if fewer than 2 periods.
        """
        revenues = [r.get("service_revenue", 0.0) for r in rows]
        if len(revenues) < 2:
            return 0.0
        x = np.arange(len(revenues), dtype=float)
        y = np.array(revenues, dtype=float)
        # Simple least-squares slope
        x_mean, y_mean = x.mean(), y.mean()
        numerator = ((x - x_mean) * (y - y_mean)).sum()
        denominator = ((x - x_mean) ** 2).sum()
        return round(float(numerator / denominator) if denominator != 0 else 0.0, 2)

    @staticmethod
    def _monthly_text(row: dict, trend_slope: float) -> str:
        """Human-readable embedding text for a monthly summary row."""
        mom = row.get("mom_growth_pct")
        mom_str = f", {mom:+.1f}% vs previous month" if mom is not None else ""
        trend_str = (
            "Revenue trend is growing."
            if trend_slope > 0
            else ("Revenue trend is declining." if trend_slope < 0 else "Revenue is flat.")
        )
        return (
            f"Revenue for {row['period']}: "
            f"${row.get('service_revenue', 0):,.2f} service revenue{mom_str}. "
            f"{row.get('visit_count', 0)} visits, "
            f"avg ticket ${row.get('avg_ticket', 0):,.2f}. "
            f"Tips: ${row.get('total_tips', 0):,.2f}. "
            f"Tax collected: ${row.get('total_tax', 0):,.2f}. "
            f"Discounts given: ${row.get('total_discounts', 0):,.2f}. "
            f"Gift card redemptions: ${row.get('gc_redemptions', 0):,.2f}. "
            f"Refunds: {row.get('refund_count', 0)}, cancellations: {row.get('cancel_count', 0)}. "
            f"{trend_str}"
        )