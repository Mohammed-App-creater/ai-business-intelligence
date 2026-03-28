"""
doc_generator.py
================
Warehouse → KPI templates → LLM observation → embed → vector store.
"""
from __future__ import annotations

import hashlib
import logging
from calendar import monthrange
from datetime import date, datetime
from typing import Any

from app.prompts.types import DocGenData
from app.services.db.warehouse_client import WarehouseClient
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.llm.llm_gateway import LLMGateway
from app.services.llm.types import UseCase
from app.services.vector_store import VectorStore

_DOMAIN_HANDLERS: dict[str, str] = {
    "revenue":       "_gen_revenue",
    "staff":         "_gen_staff",
    "services":      "_gen_services",
    "clients":       "_gen_clients",
    "appointments":  "_gen_appointments",
    "expenses":      "_gen_expenses",
    "reviews":       "_gen_reviews",
    "payments":      "_gen_payments",
    "campaigns":     "_gen_campaigns",
    "attendance":    "_gen_attendance",
    "subscriptions": "_gen_subscriptions",
}


class DocGenerator:
    """
    Reads from the analytics warehouse, generates human-readable summary
    documents using Python templates + LLM observations, embeds them,
    and stores them in the vector store.

    Usage:
        generator = DocGenerator(
            warehouse=wh_client,
            gateway=llm_gateway,
            embedding_client=embedding_client,
            vector_store=vector_store,
            business_type="Hair Salon",
        )
        result = await generator.generate_all(
            org_id=42,
            period_start=date(2026, 1, 1),
            months=3,
            force=False,
        )
    """

    def __init__(
        self,
        warehouse:        WarehouseClient,
        gateway:          LLMGateway,
        embedding_client: EmbeddingClient,
        vector_store:     VectorStore,
        business_type:    str = "Beauty & Wellness Business",
    ) -> None:
        self._wh      = warehouse
        self._gateway = gateway
        self._emb     = embedding_client
        self._vs      = vector_store
        self._biz_type = business_type
        self._logger  = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Period helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _norm_period_start(d: Any) -> date:
        if isinstance(d, datetime):
            d = d.date()
        return date(d.year, d.month, 1)

    @staticmethod
    def _add_months(d: date, delta: int) -> date:
        y, m = d.year, d.month + delta
        while m > 12:
            m -= 12
            y += 1
        while m < 1:
            m += 12
            y -= 1
        last = monthrange(y, m)[1]
        return date(y, m, min(d.day, last))

    def _month_periods(self, period_start: date | None, months: int) -> list[date]:
        months = max(1, months)
        if period_start is not None:
            start = self._norm_period_start(period_start)
            return [self._add_months(start, i) for i in range(months)]
        cur = self._norm_period_start(date.today())
        return [self._add_months(cur, -(months - 1) + i) for i in range(months)]

    @staticmethod
    def _period_label(d: date) -> str:
        return d.strftime("%B %Y")

    @staticmethod
    def _doc_month_suffix(d: date) -> str:
        return d.strftime("%Y_%m")

    @staticmethod
    def _f_money(x: Any) -> str:
        try:
            v = float(x or 0)
        except (TypeError, ValueError):
            return "$0"
        return f"${v:,.0f}"

    @staticmethod
    def _f_money_dec(x: Any) -> str:
        try:
            v = float(x or 0)
        except (TypeError, ValueError):
            return "$0.00"
        return f"${v:,.2f}"

    @staticmethod
    def _pct(part: float, total: float) -> str:
        if not total:
            return "N/A"
        return f"{part / total * 100:.0f}%"

    @staticmethod
    def _mom(current: float, previous: float | None) -> str:
        if previous is None or previous == 0:
            return ""
        pct = (current - previous) / previous * 100
        arrow = "▲" if pct >= 0 else "▼"
        return f"({arrow} {abs(pct):.0f}% vs ${previous:,.0f})"

    @staticmethod
    def _mom_visits(current: int, previous: int | None) -> str:
        if previous is None or previous == 0:
            return ""
        pct = (current - previous) / previous * 100
        arrow = "▲" if pct >= 0 else "▼"
        return f"({arrow} {abs(pct):.0f}% vs {int(previous)})"

    # ------------------------------------------------------------------
    # KPI block builders (pure)
    # ------------------------------------------------------------------

    def _kpi_revenue(self, current: dict, prev: dict | None = None) -> str:
        lines: list[str] = []
        gr = float(current.get("gross_revenue") or 0)
        pgr = float(prev["gross_revenue"]) if prev else None
        mom_r = self._mom(gr, pgr)
        lines.append(f"Revenue       : {self._f_money(gr)} {mom_r}".rstrip())

        tips = current.get("total_tips")
        if tips is not None:
            lines.append(f"Tips          : {self._f_money(tips)}")
        disc = current.get("total_discounts")
        if disc is not None:
            lines.append(f"Discounts     : {self._f_money(disc)}")

        vc = int(current.get("visit_count") or 0)
        pvc = int(prev["visit_count"]) if prev and prev.get("visit_count") is not None else None
        mom_v = self._mom_visits(vc, pvc)
        avg_v = current.get("avg_visit_value")
        lines.append(f"Visits        : {vc} {mom_v}".rstrip())
        if avg_v is not None:
            lines.append(f"Avg Visit     : {self._f_money_dec(avg_v)}")

        cc = int(current.get("cancelled_visit_count") or 0)
        if vc > 0 and cc > 0:
            lines.append(f"Cancel Rate   : {self._pct(cc, vc)}")

        cash = float(current.get("cash_revenue") or 0)
        card = float(current.get("card_revenue") or 0)
        gc_amt = float(current.get("total_gc_amount") or 0)
        other = float(current.get("other_revenue") or 0)
        tot_pay = cash + card + gc_amt + other
        if tot_pay > 0:
            lines.append(f"Cash          : {self._f_money(cash)} ({self._pct(cash, tot_pay)})")
            lines.append(f"Card          : {self._f_money(card)} ({self._pct(card, tot_pay)})")
            lines.append(f"Gift Card     : {self._f_money(gc_amt)}  ({self._pct(gc_amt, tot_pay)})")
        return "\n".join(lines)

    def _kpi_staff_monthly(self, rows: list[dict], period_label: str) -> str:
        if not rows:
            return f"Period         : {period_label}\nStaff Count    : 0"
        lines = [
            f"Period         : {period_label}",
            f"Staff Count    : {len(rows)}",
            "Top Performer  : "
            f"{rows[0].get('employee_name') or 'N/A'} — "
            f"{self._f_money(rows[0].get('total_revenue'))} revenue, "
            f"{float(rows[0].get('avg_rating') or 0):.1f} rating",
        ]
        for r in rows[1:]:
            indent = " " * 17
            nm = r.get("employee_name") or "N/A"
            rev = self._f_money(r.get("total_revenue"))
            ar = float(r.get("avg_rating") or 0)
            note = ""
            visits = int(r.get("total_visits") or 0)
            if visits > 0 and visits < 12:
                note = " (new)"
            lines.append(f"{indent}{nm} — {rev} revenue, {ar:.1f} rating{note}")
        total_rev = sum(float(r.get("total_revenue") or 0) for r in rows)
        ratings = [float(r["avg_rating"]) for r in rows if r.get("avg_rating") is not None]
        avg_r = sum(ratings) / len(ratings) if ratings else 0.0
        lines.append(f"Total Revenue  : {self._f_money(total_rev)}")
        lines.append(f"Avg Rating     : {avg_r:.1f}")
        return "\n".join(lines)

    def _kpi_staff_individual(self, rows: list[dict], name: str) -> str:
        if not rows:
            return f"Staff Member   : {name}\nPeriod         : N/A"
        ps_n = self._norm_period_start(max(r["period_start"] for r in rows))
        pl = self._period_label(ps_n)
        return self._kpi_staff_individual_for_month(rows, name, ps_n, pl)

    def _kpi_staff_individual_for_month(
        self, trend: list[dict], name: str, period_start: date, period_label: str
    ) -> str:
        if not trend:
            return f"Staff Member   : {name}\nPeriod         : {period_label}"
        ps = self._norm_period_start(period_start)
        by_ps = {self._norm_period_start(r["period_start"]): r for r in trend}
        cur_row = by_ps.get(ps)
        if cur_row is None:
            return f"Staff Member   : {name}\nPeriod         : {period_label}"
        prev_ps = self._add_months(ps, -1)
        prev_row = by_ps.get(prev_ps)
        rev = float(cur_row.get("total_revenue") or 0)
        prev_rev = float(prev_row["total_revenue"]) if prev_row else None
        visits = int(cur_row.get("total_visits") or 0)
        tips = cur_row.get("total_tips")
        comm = cur_row.get("total_commission")
        ar = cur_row.get("avg_rating")
        rc = int(cur_row.get("review_count") or 0)
        util = cur_row.get("utilisation_rate")
        booked = int(cur_row.get("appointments_booked") or 0)
        done = int(cur_row.get("appointments_completed") or 0)
        cancelled = int(cur_row.get("appointments_cancelled") or 0)
        lines = [
            f"Staff Member   : {name}",
            f"Period         : {period_label}",
            f"Visits         : {visits}",
            f"Revenue        : {self._f_money(rev)} {self._mom(rev, prev_rev)}".rstrip(),
        ]
        if tips is not None:
            lines.append(f"Tips           : {self._f_money(tips)}")
        if comm is not None:
            lines.append(f"Commission     : {self._f_money(comm)}")
        if ar is not None:
            lines.append(f"Rating         : {float(ar):.1f} / 5 ({rc} reviews)")
        if util is not None:
            lines.append(f"Utilisation    : {float(util):.0f}%")
        lines.append(f"Appts Booked   : {booked} | Completed: {done} | Cancelled: {cancelled}")
        return "\n".join(lines)

    def _kpi_services(self, rows: list[dict], period_label: str) -> str:
        lines = [
            f"Period         : {period_label}",
            "Top Services   :",
        ]
        for i, r in enumerate(rows[:10], start=1):
            nm = r.get("service_name") or "Service"
            bc = int(r.get("booking_count") or 0)
            rev = float(r.get("revenue") or 0)
            ap = float(r.get("avg_price") or 0)
            lines.append(f"  {i}. {nm} — {bc} bookings, {self._f_money(rev)} revenue, avg {self._f_money(ap)}")
        lines.append(f"Total Services : {len(rows)} active")
        return "\n".join(lines)

    def _kpi_clients_retention(self, summary: dict, period_label: str) -> str:
        if not summary:
            return f"Period         : {period_label}\nTotal Clients  : 0"
        total = int(summary.get("total_clients") or 0)
        active = int(summary.get("active_count") or 0)
        churned = int(summary.get("churned_count") or 0)
        af = float(summary.get("avg_visit_frequency_days") or 0)
        asp = float(summary.get("avg_spend_per_visit") or 0)
        apct = self._pct(active, total) if total else "N/A"
        cpct = self._pct(churned, total) if total else "N/A"
        return "\n".join([
            f"Period         : {period_label}",
            f"Total Clients  : {total}",
            f"Active Clients : {active} ({apct})",
            f"Churned        : {churned} ({cpct}) — last visit > 90 days ago",
            f"Avg Visit Freq : {af:.0f} days" if af else "Avg Visit Freq : N/A",
            f"Avg Spend/Visit: {self._f_money_dec(asp)}",
        ])

    def _kpi_clients_top(self, rows: list[dict], period_label: str) -> str:
        lines = [
            f"Period         : {period_label}",
            "Top 10 Clients by Lifetime Spend:",
        ]
        for r in rows[:10]:
            cid = int(r.get("customer_id") or 0)
            visits = int(r.get("total_visits") or 0)
            spend = float(r.get("total_spend") or 0)
            days = r.get("days_since_last_visit")
            if days is not None:
                tail = f"last visit {int(days)} days ago"
            else:
                tail = "last visit N/A"
            lines.append(
                f"  Client #{cid} — {visits} visits, {self._f_money(spend)} total spend, {tail}"
            )
        return "\n".join(lines)

    def _kpi_appointments(self, row: dict, period_label: str) -> str:
        if not row:
            return f"Period         : {period_label}"
        tb = int(row.get("total_booked") or 0)
        conf = int(row.get("confirmed_count") or 0)
        comp = int(row.get("completed_count") or 0)
        canc = int(row.get("cancelled_count") or 0)
        ns = int(row.get("no_show_count") or 0)
        wi = int(row.get("walkin_count") or 0)
        ab = int(row.get("app_booking_count") or 0)
        cr = float(row.get("cancellation_rate") or 0)
        cm = float(row.get("completion_rate") or 0)
        return "\n".join([
            f"Period         : {period_label}",
            f"Total Booked   : {tb}",
            f"Confirmed      : {conf} ({self._pct(conf, tb) if tb else 'N/A'})",
            f"Completed      : {comp} ({self._pct(comp, tb) if tb else 'N/A'})",
            f"Cancelled      : {canc} ({self._pct(canc, tb) if tb else 'N/A'})",
            f"No-Shows       : {ns}  ({self._pct(ns, tb) if tb else 'N/A'})",
            f"Walk-ins       : {wi}",
            f"App Bookings   : {ab}",
            f"Cancel Rate    : {cr:.0f}%",
            f"Completion Rate: {cm:.0f}%",
        ])

    def _kpi_expenses(self, rows: list[dict], total: dict | None, period_label: str) -> str:
        lines = [f"Period         : {period_label}"]
        amt_sum = sum(float(r.get("total_amount") or 0) for r in rows)
        if total and total.get("total") is not None:
            amt_sum = float(total.get("total") or amt_sum)
        lines.append(f"Total Expenses : {self._f_money(amt_sum)}")
        lines.append("Categories     :")
        for r in rows:
            nm = r.get("category_name") or "Other"
            a = float(r.get("total_amount") or 0)
            lines.append(f"  {nm} — {self._f_money(a)}  ({self._pct(a, amt_sum) if amt_sum else 'N/A'})")
        ec = int(total.get("count") or 0) if total else sum(int(r.get("expense_count") or 0) for r in rows)
        lines.append(f"Expense Count  : {ec}")
        avg_e = (amt_sum / ec) if ec else 0.0
        lines.append(f"Avg Expense    : {self._f_money_dec(avg_e)}")
        return "\n".join(lines)

    def _kpi_reviews(self, row: dict, period_label: str) -> str:
        if not row:
            return f"Period         : {period_label}"
        overall = row.get("overall_avg_rating")
        trc = int(row.get("total_review_count") or 0)
        erc = int(row.get("emp_review_count") or 0)
        ear = row.get("emp_avg_rating")
        vrc = int(row.get("visit_review_count") or 0)
        var_ = row.get("visit_avg_rating")
        grc = int(row.get("google_review_count") or 0)
        gar = row.get("google_avg_rating")
        gbad = int(row.get("google_bad_review_count") or 0)
        ol = f"{float(overall):.1f} / 5 ({trc} reviews)" if overall is not None else "N/A"
        lines = [
            f"Period         : {period_label}",
            f"Overall Rating : {ol}",
        ]
        if ear is not None:
            lines.append(f"Employee Reviews   : {erc} reviews, avg {float(ear):.1f}")
        else:
            lines.append(f"Employee Reviews   : {erc} reviews, avg N/A")
        if var_ is not None:
            lines.append(f"Visit Reviews      : {vrc} reviews, avg {float(var_):.1f}")
        else:
            lines.append(f"Visit Reviews      : {vrc} reviews, avg N/A")
        gline = f"Google Reviews     : {grc} reviews, avg {float(gar):.1f}" if gar is not None else f"Google Reviews     : {grc} reviews, avg N/A"
        if gbad:
            gline += f" ({gbad} flagged as negative)"
        lines.append(gline)
        return "\n".join(lines)

    def _kpi_payments(self, row: dict, period_label: str) -> str:
        if not row:
            return f"Period         : {period_label}"
        tot = float(row.get("total_amount") or 0)
        tc = int(row.get("total_count") or 0)
        cash = float(row.get("cash_amount") or 0)
        cc = int(row.get("cash_count") or 0)
        card = float(row.get("card_amount") or 0)
        cd = int(row.get("card_count") or 0)
        gc = float(row.get("gift_card_amount") or 0)
        gc_c = int(row.get("gift_card_count") or 0)
        oth = float(row.get("other_amount") or 0)
        oc = int(row.get("other_count") or 0)
        return "\n".join([
            f"Period         : {period_label}",
            f"Total Revenue  : {self._f_money(tot)} ({tc} transactions)",
            f"Cash           : {self._f_money(cash)} ({self._pct(cash, tot) if tot else 'N/A'}, {cc} transactions)",
            f"Card           : {self._f_money(card)} ({self._pct(card, tot) if tot else 'N/A'}, {cd} transactions)",
            f"Gift Card      : {self._f_money(gc)}   ({self._pct(gc, tot) if tot else 'N/A'},  {gc_c} transactions)",
            f"Other          : {self._f_money(oth)}     ({self._pct(oth, tot) if tot else 'N/A'},  {oc} transactions)",
        ])

    def _kpi_campaigns(self, rows: list[dict], period_label: str) -> str:
        if not rows:
            return "\n".join([
                f"Period         : {period_label}",
                "Campaigns Run  : 0",
                "Total Sent     : 0",
                "Open Rate      : N/A (avg)",
                "Click Rate     : N/A (avg)",
                "Fail Rate      : N/A  (avg)",
                'Top Campaign   : N/A',
            ])
        n = len(rows)
        tsent = sum(int(r.get("total_sent") or 0) for r in rows)
        opens = [float(r.get("open_rate") or 0) for r in rows]
        clicks = [float(r.get("click_rate") or 0) for r in rows]
        fails = [float(r.get("fail_rate") or 0) for r in rows]
        ao = sum(opens) / len(opens) if opens else 0.0
        ac = sum(clicks) / len(clicks) if clicks else 0.0
        af = sum(fails) / len(fails) if fails else 0.0
        top = max(rows, key=lambda r: float(r.get("open_rate") or 0))
        tname = top.get("campaign_name") or "Campaign"
        tor = float(top.get("open_rate") or 0)
        tcr = float(top.get("click_rate") or 0)
        return "\n".join([
            f"Period         : {period_label}",
            f"Campaigns Run  : {n}",
            f"Total Sent     : {tsent}",
            f"Open Rate      : {ao:.0f}% (avg)",
            f"Click Rate     : {ac:.0f}% (avg)",
            f"Fail Rate      : {af:.0f}%  (avg)",
            f'Top Campaign   : "{tname}" — {tor:.0f}% open rate, {tcr:.0f}% click rate',
        ])

    def _kpi_attendance(self, rows: list[dict], period_label: str) -> str:
        if not rows:
            return f"Period         : {period_label}\nTotal Staff    : 0"
        staff_n = len(rows)
        hours = [float(r.get("total_hours_worked") or 0) for r in rows]
        total_h = sum(hours)
        avg_h = total_h / staff_n if staff_n else 0.0
        by_h = sorted(rows, key=lambda r: float(r.get("total_hours_worked") or 0), reverse=True)
        most = by_h[0]
        least = by_h[-1]
        lines = [
            f"Period         : {period_label}",
            f"Total Staff    : {staff_n}",
            f"Total Hours    : {total_h:.0f} hrs across all staff",
            f"Avg Hours/Staff: {avg_h:.0f} hrs",
            f"Most Hours     : {most.get('employee_name') or 'N/A'} — {float(most.get('total_hours_worked') or 0):.0f} hrs ({int(most.get('days_worked') or 0)} days)",
            f"Least Hours    : {least.get('employee_name') or 'N/A'} — {float(least.get('total_hours_worked') or 0):.0f} hrs ({int(least.get('days_worked') or 0)} days)",
        ]
        return "\n".join(lines)

    def _kpi_subscriptions(self, row: dict, period_label: str) -> str:
        if not row:
            return f"Period               : {period_label}\nActive Subscriptions : 0"
        active = int(row.get("active_subscriptions") or 0)
        new = int(row.get("new_subscriptions") or 0)
        cancelled = int(row.get("cancelled_subscriptions") or 0)
        gross = float(row.get("gross_subscription_revenue") or 0)
        net = float(row.get("net_subscription_revenue") or 0)
        avg = float(row.get("avg_subscription_value") or 0)
        return "\n".join([
            f"Period               : {period_label}",
            f"Active Subscriptions : {active}",
            f"New This Month       : {new}",
            f"Cancelled This Month : {cancelled}",
            f"Gross MRR            : {self._f_money(gross)}",
            f"Net MRR              : {self._f_money(net)}",
            f"Avg Subscription     : {self._f_money_dec(avg)}",
        ])

    # ------------------------------------------------------------------
    # LLM + storage
    # ------------------------------------------------------------------

    async def _make_chunk_text(self, org_id: int, data: DocGenData) -> str:
        try:
            resp = await self._gateway.call_with_data(
                UseCase.DOC_GENERATION, data, str(org_id)
            )
            obs = (resp.content or "").strip()
            if obs:
                return f"{data.kpi_block.strip()}\n\nObservation:\n{obs}"
        except Exception:
            self._logger.warning(
                "LLM observation failed for domain=%s type=%s",
                data.doc_domain,
                data.doc_type,
                exc_info=True,
            )
        return data.kpi_block.strip()

    async def _store_doc(
        self,
        tenant_id:    str,
        doc_id:       str,
        doc_domain:   str,
        doc_type:     str,
        chunk_text:   str,
        period_start: date | None,
        metadata:     dict,
        force:        bool = False,
    ) -> str:
        content_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
        meta_base = dict(metadata) if metadata else {}
        if not force:
            existing = await self._vs.get_doc_ids(tenant_id, doc_domain, doc_type)
            if doc_id in existing:
                stored = await self._vs.get_doc_metadata(tenant_id, doc_id) or {}
                if stored.get("content_hash") == content_hash:
                    return "skipped"
        full_meta = {**meta_base, "content_hash": content_hash}
        try:
            vec = await self._emb.embed(chunk_text)
            await self._vs.upsert(
                tenant_id=tenant_id,
                doc_id=doc_id,
                doc_domain=doc_domain,
                doc_type=doc_type,
                chunk_text=chunk_text,
                embedding=vec,
                period_start=period_start,
                metadata=full_meta,
            )
        except Exception:
            self._logger.exception("embed/upsert failed doc_id=%s", doc_id)
            return "failed"
        return "created"

    async def generate_domain(
        self,
        org_id:       int,
        domain:       str,
        period_start: date | None,
        months:       int = 3,
        force:        bool = False,
    ) -> tuple[int, int, int]:
        handler_name = _DOMAIN_HANDLERS.get(domain)
        if not handler_name:
            return 0, 0, 0
        handler = getattr(self, handler_name)
        return await handler(org_id, period_start, months, force)

    async def generate_all(
        self,
        org_id:       int,
        period_start: date | None = None,
        months:       int = 3,
        domain:       str | None = None,
        force:        bool = False,
    ) -> dict:
        domains = [domain] if domain else list(_DOMAIN_HANDLERS.keys())
        created = skipped = failed = 0
        errors: list[str] = []
        for dom in domains:
            try:
                c, s, f = await self.generate_domain(
                    org_id, dom, period_start, months, force
                )
                created += c
                skipped += s
                failed += f
            except Exception as e:
                self._logger.exception("generate_domain failed domain=%s", dom)
                errors.append(f"{dom}: {e}")
                failed += 1
        return {
            "org_id":       org_id,
            "docs_created": created,
            "docs_skipped": skipped,
            "docs_failed":  failed,
            "errors":       errors,
        }

    # ------------------------------------------------------------------
    # Domain generators
    # ------------------------------------------------------------------

    async def _gen_revenue(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        periods = self._month_periods(period_start, months)
        rows = await self._wh.revenue.get_monthly_trend(org_id, months=months + 2)
        by_ps: dict[date, dict] = {}
        for r in rows:
            ps = r["period_start"]
            if hasattr(ps, "replace"):
                by_ps[self._norm_period_start(ps)] = r
        tenant = str(org_id)
        for ps in periods:
            cur = by_ps.get(self._norm_period_start(ps))
            if not cur:
                continue
            prev_ps = self._add_months(self._norm_period_start(ps), -1)
            prev = by_ps.get(prev_ps)
            kpi = self._kpi_revenue(cur, prev)
            pl = self._period_label(ps)
            doc_id = f"{org_id}_revenue_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="revenue",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            status = await self._store_doc(
                tenant, doc_id, "revenue", "monthly_summary", chunk, ps, {}, force
            )
            if status == "created":
                created += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_staff(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        periods = self._month_periods(period_start, months)
        tenant = str(org_id)
        for ps in periods:
            pl = self._period_label(ps)
            monthly = await self._wh.staff.get_staff_monthly_performance(org_id, ps)
            if monthly:
                kpi_m = self._kpi_staff_monthly(monthly, pl)
                doc_id = f"{org_id}_staff_monthly_{self._doc_month_suffix(ps)}"
                data = DocGenData(
                    business_id=str(org_id),
                    business_type=self._biz_type,
                    period=pl,
                    doc_domain="staff",
                    doc_type="monthly_summary",
                    kpi_block=kpi_m,
                )
                chunk = await self._make_chunk_text(org_id, data)
                st = await self._store_doc(
                    tenant, doc_id, "staff", "monthly_summary", chunk, ps, {}, force
                )
                if st == "created":
                    created += 1
                elif st == "skipped":
                    skipped += 1
                else:
                    failed += 1
            for row in monthly:
                eid = int(row.get("employee_id") or 0)
                name = str(row.get("employee_name") or "")
                trend = await self._wh.staff.get_staff_trend(org_id, eid, months=24)
                kpi_i = self._kpi_staff_individual_for_month(trend, name, ps, pl)
                doc_i = f"{org_id}_staff_{eid}_{self._doc_month_suffix(ps)}"
                data_i = DocGenData(
                    business_id=str(org_id),
                    business_type=self._biz_type,
                    period=pl,
                    doc_domain="staff",
                    doc_type="individual",
                    kpi_block=kpi_i,
                    entity_name=name,
                )
                chunk_i = await self._make_chunk_text(org_id, data_i)
                st_i = await self._store_doc(
                    tenant, doc_i, "staff", "individual", chunk_i, ps, {}, force
                )
                if st_i == "created":
                    created += 1
                elif st_i == "skipped":
                    skipped += 1
                else:
                    failed += 1
        return created, skipped, failed

    async def _gen_services(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            rows = await self._wh.services.get_service_monthly_performance(org_id, ps)
            if not rows:
                continue
            kpi = self._kpi_services(rows, pl)
            doc_id = f"{org_id}_services_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="services",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "services", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_clients(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            summary = await self._wh.clients.get_retention_summary(org_id)
            top = await self._wh.clients.get_top_clients_by_spend(org_id, limit=10)
            if summary:
                kpi_r = self._kpi_clients_retention(summary, pl)
                doc_r = f"{org_id}_clients_retention_{self._doc_month_suffix(ps)}"
                data_r = DocGenData(
                    business_id=str(org_id),
                    business_type=self._biz_type,
                    period=pl,
                    doc_domain="clients",
                    doc_type="retention_summary",
                    kpi_block=kpi_r,
                )
                ch = await self._make_chunk_text(org_id, data_r)
                st = await self._store_doc(
                    tenant, doc_r, "clients", "retention_summary", ch, ps, {}, force
                )
                if st == "created":
                    created += 1
                elif st == "skipped":
                    skipped += 1
                else:
                    failed += 1
            if top:
                kpi_t = self._kpi_clients_top(top, pl)
                doc_t = f"{org_id}_clients_top_{self._doc_month_suffix(ps)}"
                data_t = DocGenData(
                    business_id=str(org_id),
                    business_type=self._biz_type,
                    period=pl,
                    doc_domain="clients",
                    doc_type="top_spenders",
                    kpi_block=kpi_t,
                )
                ch2 = await self._make_chunk_text(org_id, data_t)
                st2 = await self._store_doc(
                    tenant, doc_t, "clients", "top_spenders", ch2, ps, {}, force
                )
                if st2 == "created":
                    created += 1
                elif st2 == "skipped":
                    skipped += 1
                else:
                    failed += 1
        return created, skipped, failed

    async def _gen_appointments(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            row = await self._wh.appointments.get_appointment_monthly_summary(org_id, ps)
            if not row:
                continue
            kpi = self._kpi_appointments(row, pl)
            doc_id = f"{org_id}_appointments_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="appointments",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "appointments", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_expenses(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            raw = await self._wh.expenses.get_expense_monthly_summary(org_id, ps)
            rows = [r for r in raw if int(r.get("location_id") or 0) == 0]
            if not rows:
                rows = raw
            total = await self._wh.expenses.get_expense_total(org_id, ps)
            if not rows and not (total and float(total.get("total") or 0)):
                continue
            kpi = self._kpi_expenses(rows, total, pl)
            doc_id = f"{org_id}_expenses_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="expenses",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "expenses", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_reviews(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            row = await self._wh.reviews.get_review_monthly_summary(org_id, ps)
            if not row:
                continue
            kpi = self._kpi_reviews(row, pl)
            doc_id = f"{org_id}_reviews_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="reviews",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "reviews", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_payments(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            row = await self._wh.payments.get_payment_monthly_breakdown(org_id, ps)
            if not row:
                continue
            kpi = self._kpi_payments(row, pl)
            doc_id = f"{org_id}_payments_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="payments",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "payments", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_campaigns(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            rows = await self._wh.campaigns.get_campaign_monthly_summary(org_id, ps)
            kpi = self._kpi_campaigns(rows, pl)
            doc_id = f"{org_id}_campaigns_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="campaigns",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "campaigns", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_attendance(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            raw = await self._wh.attendance.get_staff_attendance_monthly(org_id, ps)
            rows = [r for r in raw if int(r.get("location_id") or 0) == 0]
            if not rows:
                rows = raw
            if not rows:
                continue
            kpi = self._kpi_attendance(rows, pl)
            doc_id = f"{org_id}_attendance_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="attendance",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "attendance", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed

    async def _gen_subscriptions(
        self, org_id: int, period_start: date | None, months: int, force: bool
    ) -> tuple[int, int, int]:
        created = skipped = failed = 0
        tenant = str(org_id)
        for ps in self._month_periods(period_start, months):
            pl = self._period_label(ps)
            row = await self._wh.subscriptions.get_subscription_monthly_summary(org_id, ps)
            if not row:
                continue
            kpi = self._kpi_subscriptions(row, pl)
            doc_id = f"{org_id}_subscriptions_monthly_{self._doc_month_suffix(ps)}"
            data = DocGenData(
                business_id=str(org_id),
                business_type=self._biz_type,
                period=pl,
                doc_domain="subscriptions",
                doc_type="monthly_summary",
                kpi_block=kpi,
            )
            chunk = await self._make_chunk_text(org_id, data)
            st = await self._store_doc(
                tenant, doc_id, "subscriptions", "monthly_summary", chunk, ps, {}, force
            )
            if st == "created":
                created += 1
            elif st == "skipped":
                skipped += 1
            else:
                failed += 1
        return created, skipped, failed
