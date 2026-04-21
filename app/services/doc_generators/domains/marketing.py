"""
app/services/doc_generators/domains/marketing.py
=================================================
Builds embedding-ready RAG document chunks from the 3 marketing warehouse
streams produced by MarketingExtractor.run():
    - campaign_summary   (per-execution rows)
    - channel_monthly    (per-period rollups with volume + perf + unsub)
    - promo_attribution  (per-campaign-per-period-per-location revenue)

Follows the same contract as domains/clients.py and domains/appointments.py:

    async def generate_marketing_docs(
        org_id: int,
        warehouse_rows: dict,     # the dict returned by MarketingExtractor.run()
        embedding_client: EmbeddingClient,
        vector_store: VectorStore,
        force: bool = False,
    ) -> dict

Returns: {"docs_created": int, "docs_skipped": int, "docs_failed": int}

Document strategy (14 chunk types covering the 34 questions):

  Per-period chunks (one per month in range):
    1.  monthly_campaigns_summary   — all campaigns that ran in the period
    2.  channel_mix                  — email vs SMS volume + engagement rates
    3.  unsubscribe_snapshot         — list health (contactable, unsub delta)
    4.  promo_redemptions            — campaigns with revenue attribution
    5.  location_promo_breakdown     — per-branch promo revenue (Q29, Q30)

  Rolling / cross-period chunks (one per business):
    6.  campaign_performance_ranked  — top campaigns by open/click/reach
    7.  recurring_campaigns_health   — recurring campaign performance over time
    8.  template_format_analysis     — best templates by engagement
    9.  day_of_week_analysis         — best send day historically
    10. list_growth_trend            — 6-month list size + unsub trajectory
    11. expired_but_active_campaigns — workflow health flag (Q4, Q8)

  Campaign spotlight chunks (one per notable campaign):
    12. campaign_spotlight           — deep dive on best/worst performers

  Question-specific chunks:
    13. latest_campaign_report       — THE "last campaign" for Q5, Q6, Q9
    14. roi_analysis                 — did-it-pay-for-itself rollup
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

DOMAIN = "marketing"


# ═════════════════════════════════════════════════════════════════════════════
# Public entry point
# ═════════════════════════════════════════════════════════════════════════════

async def generate_marketing_docs(
    org_id: int,
    warehouse_rows: dict,
    embedding_client: Any,
    vector_store: Any,
    force: bool = False,
) -> dict:
    """
    Build all marketing chunks, embed them, upsert into pgvector.

    Parameters
    ----------
    warehouse_rows:
        dict returned by MarketingExtractor.run() with keys:
            "campaign_summary", "channel_monthly", "promo_attribution"
    """
    tenant = str(org_id)
    campaign_rows = warehouse_rows.get("campaign_summary") or []
    channel_rows  = warehouse_rows.get("channel_monthly")  or []
    promo_rows    = warehouse_rows.get("promo_attribution") or []

    if not any([campaign_rows, channel_rows, promo_rows]):
        logger.info(
            "marketing_docs: no warehouse data for org=%d — skipping", org_id,
        )
        return {"docs_created": 0, "docs_skipped": 0, "docs_failed": 0}

    docs: list[dict] = []

    # ── Per-period chunks ──────────────────────────────────────────────────
    docs.extend(_build_monthly_campaign_summaries(org_id, campaign_rows))
    docs.extend(_build_channel_mix_chunks(org_id, channel_rows))
    docs.extend(_build_unsubscribe_snapshots(org_id, channel_rows))
    docs.extend(_build_promo_redemption_chunks(org_id, promo_rows))
    docs.extend(_build_location_promo_chunks(org_id, promo_rows))

    # ── Rolling / cross-period chunks ──────────────────────────────────────
    docs.extend(_build_campaign_performance_ranked(org_id, campaign_rows))
    docs.extend(_build_recurring_campaigns_health(org_id, campaign_rows))
    docs.extend(_build_template_format_analysis(org_id, campaign_rows))
    docs.extend(_build_day_of_week_analysis(org_id, campaign_rows))
    docs.extend(_build_list_growth_trend(org_id, channel_rows))
    docs.extend(_build_expired_but_active(org_id, campaign_rows))

    # ── Campaign spotlights ────────────────────────────────────────────────
    docs.extend(_build_campaign_spotlights(org_id, campaign_rows, promo_rows))

    # ── Question-targeted ──────────────────────────────────────────────────
    docs.extend(_build_latest_campaign_report(org_id, campaign_rows, promo_rows))
    docs.extend(_build_roi_analysis(org_id, campaign_rows, promo_rows))

    # ── Embed + upsert each chunk ──────────────────────────────────────────
    created = skipped = failed = 0
    for d in docs:
        status = await _embed_and_upsert(
            tenant=tenant,
            embedding_client=embedding_client,
            vector_store=vector_store,
            doc=d,
            force=force,
        )
        if status == "created":
            created += 1
        elif status == "skipped":
            skipped += 1
        else:
            failed += 1

    logger.info(
        "marketing_docs: org=%d created=%d skipped=%d failed=%d",
        org_id, created, skipped, failed,
    )
    return {
        "docs_created": created,
        "docs_skipped": skipped,
        "docs_failed":  failed,
    }


# ═════════════════════════════════════════════════════════════════════════════
# Chunk builders — per-period
# ═════════════════════════════════════════════════════════════════════════════

def _build_monthly_campaign_summaries(org_id: int, rows: list[dict]) -> list[dict]:
    """One chunk per period summarising all campaigns that ran."""
    docs = []
    by_period: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("period") is not None and r.get("execution_date") is not None:
            by_period[str(r["period"])[:10]].append(r)

    for period, period_rows in sorted(by_period.items(), reverse=True):
        pl = _period_label(period)
        lines = [
            f"Period              : {pl}",
            f"Campaigns Executed  : {len({r['campaign_id'] for r in period_rows})}",
            f"Executions This Month: {len(period_rows)}",
            "",
            "Campaign rollup:",
        ]

        # Sort by delivered (reach) desc
        sorted_rows = sorted(period_rows, key=lambda r: -(r.get("delivered") or 0))
        for r in sorted_rows:
            nm = r.get("campaign_name") or "Campaign"
            ch = r.get("channel") or "unknown"
            dlv = int(r.get("delivered") or 0)
            op = _fmt_rate(r.get("open_rate_pct"))
            ck = _fmt_rate(r.get("click_rate_pct"))
            rec = " [recurring]" if r.get("is_recurring") else ""
            promo = f", promo={r['promo_code_string']}" if r.get("promo_code_string") else ""
            lines.append(
                f"  • {nm} ({ch}){rec} — delivered {dlv}, "
                f"open {op}, click {ck}{promo}"
            )

        # Period aggregates
        tot_sent = sum(int(r.get("total_sent") or 0) for r in period_rows)
        tot_delivered = sum(int(r.get("delivered") or 0) for r in period_rows)
        tot_failed = sum(int(r.get("failed") or 0) for r in period_rows)
        overall_delivery = _safe_pct(tot_delivered, tot_sent)
        lines.append("")
        lines.append(f"Total Sent          : {tot_sent}")
        lines.append(f"Total Delivered     : {tot_delivered}")
        lines.append(f"Total Failed        : {tot_failed}")
        lines.append(f"Overall Delivery    : {overall_delivery}")

        docs.append({
            "doc_id":       f"{org_id}_marketing_monthly_summary_{_period_suffix(period)}",
            "doc_domain":   DOMAIN,
            "doc_type":     "monthly_campaign_summary",
            "period_start": _to_date(period),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"period": period, "campaign_count": len({r["campaign_id"] for r in period_rows})},
        })
    return docs


def _build_channel_mix_chunks(org_id: int, rows: list[dict]) -> list[dict]:
    """One chunk per period — email vs SMS volume + engagement."""
    docs = []
    for r in rows:
        period = str(r.get("period") or "")[:10]
        if not period:
            continue
        pl = _period_label(period)
        emails = int(r.get("emails_sent") or 0)
        sms = int(r.get("sms_sent") or 0)
        total = emails + sms
        email_pct = _safe_pct(emails, total)
        sms_pct = _safe_pct(sms, total)
        e_open = _fmt_rate(r.get("email_open_rate_pct"))
        e_click = _fmt_rate(r.get("email_click_rate_pct"))
        s_open = _fmt_rate(r.get("sms_open_rate_pct"))
        s_click = _fmt_rate(r.get("sms_click_rate_pct"))
        e_mom = _fmt_rate_signed(r.get("emails_mom_pct"))
        s_mom = _fmt_rate_signed(r.get("sms_mom_pct"))

        lines = [
            f"Period              : {pl}",
            "",
            "── Email channel ──",
            f"Emails sent         : {emails} ({email_pct} of outreach)",
            f"MoM change          : {e_mom}",
            f"Open rate           : {e_open}",
            f"Click rate          : {e_click}",
            f"Campaigns run       : {r.get('email_campaigns_run') or 0}",
            "",
            "── SMS channel ──",
            f"SMS sent            : {sms} ({sms_pct} of outreach)",
            f"MoM change          : {s_mom}",
            f"Open rate           : {s_open}  (SMS has no open tracking)",
            f"Click rate          : {s_click}",
            f"Campaigns run       : {r.get('sms_campaigns_run') or 0}",
        ]
        docs.append({
            "doc_id":       f"{org_id}_marketing_channel_mix_{_period_suffix(period)}",
            "doc_domain":   DOMAIN,
            "doc_type":     "channel_mix",
            "period_start": _to_date(period),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"period": period, "emails_sent": emails, "sms_sent": sms},
        })
    return docs


def _build_unsubscribe_snapshots(org_id: int, rows: list[dict]) -> list[dict]:
    """One chunk per period — unsubscribe snapshot + net delta."""
    docs = []
    for r in rows:
        period = str(r.get("period") or "")[:10]
        if not period:
            continue
        pl = _period_label(period)
        e_unsub = r.get("email_unsubscribed_count")
        s_unsub = r.get("sms_unsubscribed_count")
        total = r.get("total_contacts")
        e_contact = r.get("email_contactable")
        s_contact = r.get("sms_contactable")
        e_delta = r.get("email_net_unsub_delta")
        s_delta = r.get("sms_net_unsub_delta")
        contact_mom = _fmt_rate_signed(r.get("email_contactable_mom_pct"))

        lines = [
            f"Period                   : {pl}",
            "",
            "── List snapshot at period end ──",
            f"Total contacts           : {_fmt_int(total)}",
            f"Email unsubscribed       : {_fmt_int(e_unsub)}",
            f"SMS unsubscribed         : {_fmt_int(s_unsub)}",
            f"Email-contactable        : {_fmt_int(e_contact)}",
            f"SMS-contactable          : {_fmt_int(s_contact)}",
            "",
            "── This period's net delta ──",
            f"Email net unsubscribes   : {_fmt_int_signed(e_delta)} new opt-outs this month",
            f"SMS net unsubscribes     : {_fmt_int_signed(s_delta)} new opt-outs this month",
            f"Email-contactable MoM    : {contact_mom}",
        ]
        docs.append({
            "doc_id":       f"{org_id}_marketing_unsub_snapshot_{_period_suffix(period)}",
            "doc_domain":   DOMAIN,
            "doc_type":     "unsubscribe_snapshot",
            "period_start": _to_date(period),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"period": period, "email_net_unsub_delta": e_delta},
        })
    return docs


def _build_promo_redemption_chunks(org_id: int, rows: list[dict]) -> list[dict]:
    """One chunk per period — all campaign promo redemptions rolled up."""
    docs = []
    by_period: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        period = str(r.get("period") or "")[:10]
        if period:
            by_period[period].append(r)

    for period, period_rows in sorted(by_period.items(), reverse=True):
        pl = _period_label(period)

        # Aggregate per campaign (summed across locations)
        by_campaign: dict[int, dict] = {}
        for r in period_rows:
            cid = r["campaign_id"]
            if cid not in by_campaign:
                by_campaign[cid] = {
                    "name":       r.get("campaign_name") or "Campaign",
                    "promo":      r.get("promo_code_string") or "",
                    "redemptions": 0,
                    "revenue":     0.0,
                    "discount":    0.0,
                    "net":         0.0,
                    "audience":   r.get("audience_size") or 0,
                }
            by_campaign[cid]["redemptions"] += int(r.get("redemptions") or 0)
            by_campaign[cid]["revenue"]     += float(r.get("attributed_revenue") or 0)
            by_campaign[cid]["discount"]    += float(r.get("total_discount_given") or 0)
            by_campaign[cid]["net"]         += float(r.get("net_revenue_after_discount") or 0)

        lines = [f"Period               : {pl}", ""]
        tot_red = sum(c["redemptions"] for c in by_campaign.values())
        tot_rev = sum(c["net"]         for c in by_campaign.values())
        lines.append(f"Total redemptions    : {tot_red}")
        lines.append(f"Total net revenue    : {_f_money(tot_rev)}")
        lines.append("")
        lines.append("Per-campaign:")

        for c in sorted(by_campaign.values(), key=lambda x: -x["net"]):
            conv = _safe_pct(c["redemptions"], c["audience"] or 0, decimals=2)
            lines.append(
                f"  • {c['name']} (promo={c['promo']}) — "
                f"{c['redemptions']} redemptions from audience of {c['audience']} "
                f"({conv} conversion), {_f_money(c['net'])} net revenue, "
                f"{_f_money(c['discount'])} discount given"
            )

        docs.append({
            "doc_id":       f"{org_id}_marketing_promo_redemptions_{_period_suffix(period)}",
            "doc_domain":   DOMAIN,
            "doc_type":     "promo_redemptions",
            "period_start": _to_date(period),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"period": period, "total_redemptions": tot_red},
        })
    return docs


def _build_location_promo_chunks(org_id: int, rows: list[dict]) -> list[dict]:
    """One chunk per period — per-branch promo revenue breakdown (Q29, Q30)."""
    docs = []
    by_period: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        period = str(r.get("period") or "")[:10]
        if period:
            by_period[period].append(r)

    for period, period_rows in sorted(by_period.items(), reverse=True):
        pl = _period_label(period)

        # Aggregate per location
        by_loc: dict[int, dict] = {}
        for r in period_rows:
            lid = r["location_id"]
            if lid not in by_loc:
                by_loc[lid] = {
                    "redemptions": 0, "revenue": 0.0,
                    "net": 0.0, "discount": 0.0,
                }
            by_loc[lid]["redemptions"] += int(r.get("redemptions") or 0)
            by_loc[lid]["revenue"]     += float(r.get("attributed_revenue") or 0)
            by_loc[lid]["net"]         += float(r.get("net_revenue_after_discount") or 0)
            by_loc[lid]["discount"]    += float(r.get("total_discount_given") or 0)

        lines = [f"Period             : {pl}", "", "Per-location promo attribution:"]
        for lid, agg in sorted(by_loc.items(), key=lambda x: -x[1]["net"]):
            lines.append(
                f"  • Location {lid}: {agg['redemptions']} redemptions, "
                f"{_f_money(agg['net'])} net revenue, "
                f"{_f_money(agg['discount'])} discount given"
            )

        # Rank the best-converting location
        if by_loc:
            best = max(by_loc.items(), key=lambda x: x[1]["redemptions"])
            lines.append("")
            lines.append(f"Top-converting branch: Location {best[0]} with {best[1]['redemptions']} redemptions")

        docs.append({
            "doc_id":       f"{org_id}_marketing_location_promo_{_period_suffix(period)}",
            "doc_domain":   DOMAIN,
            "doc_type":     "location_promo",
            "period_start": _to_date(period),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"period": period},
        })
    return docs


# ═════════════════════════════════════════════════════════════════════════════
# Chunk builders — rolling / cross-period
# ═════════════════════════════════════════════════════════════════════════════

def _build_campaign_performance_ranked(org_id: int, rows: list[dict]) -> list[dict]:
    """One doc per year — top campaigns by open, click, reach."""
    docs = []

    # Group by year from period
    by_year: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("period") and r.get("execution_date"):
            y = int(str(r["period"])[:4])
            by_year[y].append(r)

    for year, year_rows in sorted(by_year.items(), reverse=True):
        # Best by open (email only — SMS open is NULL)
        email_rows = [r for r in year_rows if r.get("channel") == "email"
                      and r.get("open_rate_pct") is not None]
        top_open = sorted(email_rows, key=lambda r: -(r.get("open_rate_pct") or 0))[:5]
        top_click = sorted(email_rows, key=lambda r: -(r.get("click_rate_pct") or 0))[:5]
        top_reach = sorted(year_rows, key=lambda r: -(r.get("delivered") or 0))[:5]

        lines = [
            f"Year                : {year}",
            f"Total executions    : {len(year_rows)}",
            "",
            "Top 5 campaigns by open rate (email):",
        ]
        for r in top_open:
            lines.append(
                f"  • {r['campaign_name']} — {_fmt_rate(r['open_rate_pct'])} "
                f"(delivered {r.get('delivered', 0)}, {r.get('execution_date')})"
            )
        lines.append("")
        lines.append("Top 5 campaigns by click rate (email):")
        for r in top_click:
            lines.append(
                f"  • {r['campaign_name']} — {_fmt_rate(r['click_rate_pct'])} "
                f"(delivered {r.get('delivered', 0)}, {r.get('execution_date')})"
            )
        lines.append("")
        lines.append("Top 5 campaigns by reach (all channels):")
        for r in top_reach:
            lines.append(
                f"  • {r['campaign_name']} ({r.get('channel', 'unknown')}) — "
                f"reached {r.get('delivered', 0)} customers ({r.get('execution_date')})"
            )

        docs.append({
            "doc_id":       f"{org_id}_marketing_performance_ranked_{year}",
            "doc_domain":   DOMAIN,
            "doc_type":     "campaign_performance_ranked",
            "period_start": date(year, 1, 1),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"year": year},
        })
    return docs


def _build_recurring_campaigns_health(org_id: int, rows: list[dict]) -> list[dict]:
    """Q14, Q25 — recurring campaign performance over time."""
    recurring = [r for r in rows if r.get("is_recurring") and r.get("execution_date")]
    if not recurring:
        return []

    by_campaign: dict[int, list[dict]] = defaultdict(list)
    for r in recurring:
        by_campaign[r["campaign_id"]].append(r)

    lines = ["Recurring campaigns health report", "=" * 40, ""]
    for cid, exec_rows in by_campaign.items():
        exec_rows.sort(key=lambda r: r.get("execution_date") or "")
        nm = exec_rows[0].get("campaign_name") or "Campaign"
        ch = exec_rows[0].get("channel") or "unknown"
        first = exec_rows[0].get("execution_date")
        last = exec_rows[-1].get("execution_date")
        count = len(exec_rows)

        # Compute average open/click over all runs (email only)
        if ch == "email":
            opens = [r["open_rate_pct"] for r in exec_rows
                     if r.get("open_rate_pct") is not None]
            clicks = [r["click_rate_pct"] for r in exec_rows
                      if r.get("click_rate_pct") is not None]
            avg_open = f"{sum(opens)/len(opens):.2f}%" if opens else "N/A"
            avg_click = f"{sum(clicks)/len(clicks):.2f}%" if clicks else "N/A"
        else:
            clicks = [r["click_rate_pct"] for r in exec_rows
                      if r.get("click_rate_pct") is not None]
            avg_open = "N/A (SMS)"
            avg_click = f"{sum(clicks)/len(clicks):.2f}%" if clicks else "N/A"

        lines.append(f"• {nm} ({ch})")
        lines.append(f"    Runs          : {count} (first {first}, last {last})")
        lines.append(f"    Avg open rate : {avg_open}")
        lines.append(f"    Avg click rate: {avg_click}")
        lines.append("")

    return [{
        "doc_id":       f"{org_id}_marketing_recurring_health",
        "doc_domain":   DOMAIN,
        "doc_type":     "recurring_campaigns_health",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"recurring_count": len(by_campaign)},
    }]


def _build_template_format_analysis(org_id: int, rows: list[dict]) -> list[dict]:
    """Q26 — which campaign template format performs best."""
    by_template: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        tpl = r.get("template_format_name")
        if tpl and r.get("channel") == "email" and r.get("open_rate_pct") is not None:
            by_template[tpl].append(r)
    if not by_template:
        return []

    lines = ["Template format analysis (email campaigns)", "=" * 44, ""]
    rankings = []
    for tpl, exec_rows in by_template.items():
        opens = [r["open_rate_pct"] for r in exec_rows]
        clicks = [r["click_rate_pct"] for r in exec_rows if r.get("click_rate_pct") is not None]
        avg_open = sum(opens) / len(opens) if opens else 0
        avg_click = sum(clicks) / len(clicks) if clicks else 0
        rankings.append((tpl, avg_open, avg_click, len(exec_rows)))

    rankings.sort(key=lambda x: -x[1])  # by open rate desc
    for i, (tpl, op, cl, n) in enumerate(rankings, start=1):
        lines.append(
            f"{i}. {tpl} — {n} uses, avg open {op:.2f}%, avg click {cl:.2f}%"
        )

    return [{
        "doc_id":       f"{org_id}_marketing_template_analysis",
        "doc_domain":   DOMAIN,
        "doc_type":     "template_format_analysis",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"template_count": len(rankings)},
    }]


def _build_day_of_week_analysis(org_id: int, rows: list[dict]) -> list[dict]:
    """Q33 — best historical send day."""
    email_rows = [r for r in rows
                  if r.get("channel") == "email"
                  and r.get("execution_date")
                  and r.get("open_rate_pct") is not None]
    if not email_rows:
        return []

    DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
            "Saturday", "Sunday"]
    by_dow: dict[int, list[float]] = defaultdict(list)
    by_dow_clicks: dict[int, list[float]] = defaultdict(list)
    for r in email_rows:
        try:
            d = date.fromisoformat(str(r["execution_date"])[:10])
        except ValueError:
            continue
        by_dow[d.weekday()].append(r["open_rate_pct"])
        if r.get("click_rate_pct") is not None:
            by_dow_clicks[d.weekday()].append(r["click_rate_pct"])

    lines = ["Best day of week to send email campaigns",
             "=" * 42,
             "",
             "Based on historical execution data (email only):"]
    stats = []
    for dow in range(7):
        opens = by_dow.get(dow, [])
        clicks = by_dow_clicks.get(dow, [])
        if not opens:
            continue
        avg_o = sum(opens) / len(opens)
        avg_c = sum(clicks) / len(clicks) if clicks else 0
        stats.append((dow, avg_o, avg_c, len(opens)))

    stats.sort(key=lambda x: -x[1])
    for i, (dow, o, c, n) in enumerate(stats, start=1):
        lines.append(f"{i}. {DAYS[dow]:10s} — {n} sends, avg open {o:.2f}%, avg click {c:.2f}%")

    if stats:
        best = stats[0]
        lines.append("")
        lines.append(
            f"Historical best send day: {DAYS[best[0]]} "
            f"(avg open rate {best[1]:.2f}% across {best[3]} sends)"
        )

    return [{
        "doc_id":       f"{org_id}_marketing_day_of_week",
        "doc_domain":   DOMAIN,
        "doc_type":     "day_of_week_analysis",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"sample_size": len(email_rows)},
    }]


def _build_list_growth_trend(org_id: int, rows: list[dict]) -> list[dict]:
    """Q28, Q34 — 6-month trend of contactable list and unsub deltas."""
    if not rows:
        return []
    # rows come back DESC; use asc for trend
    asc = sorted(rows, key=lambda r: str(r.get("period") or ""))
    if not asc:
        return []

    lines = ["Email list health trend", "=" * 23, ""]
    lines.append("Period      | Contactable | Unsubbed | Net Δ | MoM %")
    lines.append("-" * 58)
    for r in asc:
        period = str(r.get("period") or "")[:10]
        c = r.get("email_contactable")
        u = r.get("email_unsubscribed_count")
        d = r.get("email_net_unsub_delta")
        m = r.get("email_contactable_mom_pct")
        lines.append(
            f"{period}  | {_pad(c, 11)} | {_pad(u, 8)} | "
            f"{_pad_signed(d, 5)} | {_fmt_rate_signed(m)}"
        )

    # Recent direction
    if len(asc) >= 2:
        first = asc[0].get("email_contactable")
        last = asc[-1].get("email_contactable")
        if first is not None and last is not None:
            delta = last - first
            pct = (delta / first * 100) if first else 0
            direction = "growing" if delta > 0 else "shrinking" if delta < 0 else "flat"
            lines.append("")
            lines.append(
                f"List is {direction} — contactable email audience "
                f"went from {first} to {last} ({delta:+d}, {pct:+.2f}%)"
            )

    return [{
        "doc_id":       f"{org_id}_marketing_list_growth",
        "doc_domain":   DOMAIN,
        "doc_type":     "list_growth_trend",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"period_count": len(asc)},
    }]


def _build_expired_but_active(org_id: int, rows: list[dict]) -> list[dict]:
    """Q4 + workflow health — campaigns flagged is_expired_but_active."""
    flagged = [r for r in rows if r.get("is_expired_but_active") == 1]
    if not flagged:
        return []

    # Dedupe by campaign_id (the never-executed phantom row per campaign)
    seen = set()
    unique = []
    for r in flagged:
        if r["campaign_id"] not in seen:
            seen.add(r["campaign_id"])
            unique.append(r)

    lines = [
        "Workflow health: campaigns marked active but past expiration",
        "=" * 60,
        "",
        "These campaigns are flagged is_expired_but_active=1.",
        "They are set to active but their expiration date has passed, so they",
        "won't actually send. Review and either extend or deactivate.",
        "",
    ]
    for r in unique:
        lines.append(
            f"  • {r['campaign_name']} (ID {r['campaign_id']}) — "
            f"expired {r.get('campaign_expiration')}, still marked active"
        )

    return [{
        "doc_id":       f"{org_id}_marketing_expired_active",
        "doc_domain":   DOMAIN,
        "doc_type":     "expired_but_active_campaigns",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"flagged_count": len(unique)},
    }]


def _build_campaign_spotlights(
    org_id: int,
    campaign_rows: list[dict],
    promo_rows: list[dict],
) -> list[dict]:
    """One chunk per notable campaign (top + bottom performer by open rate)."""
    # Only campaigns that actually ran
    executed = [r for r in campaign_rows
                if r.get("execution_date") and r.get("channel") == "email"
                and r.get("open_rate_pct") is not None]
    if not executed:
        return []

    # Take top 2 and bottom 1 to keep embeddings focused
    by_open = sorted(executed, key=lambda r: -(r.get("open_rate_pct") or 0))
    spotlights = by_open[:2] + by_open[-1:]

    # Dedupe by campaign_id — show each campaign's best single execution
    seen: set = set()
    final: list[dict] = []
    for r in spotlights:
        if r["campaign_id"] not in seen:
            seen.add(r["campaign_id"])
            final.append(r)

    # Promo revenue per campaign
    promo_by_cid: dict[int, float] = defaultdict(float)
    for p in promo_rows:
        promo_by_cid[p["campaign_id"]] += float(p.get("net_revenue_after_discount") or 0)

    docs = []
    for r in final:
        cid = r["campaign_id"]
        lines = [
            f"Campaign            : {r['campaign_name']}",
            f"Channel             : {r.get('channel')}",
            f"Execution date      : {r.get('execution_date')}",
            f"Audience size       : {r.get('audience_size')}",
            f"Recurring?          : {'Yes' if r.get('is_recurring') else 'No'}",
            f"Promo code          : {r.get('promo_code_string') or 'None'}",
            f"Template format     : {r.get('template_format_name') or 'None'}",
            "",
            "── Performance ──",
            f"Sent                : {r.get('total_sent')}",
            f"Delivered           : {r.get('delivered')} ({_fmt_rate(r.get('delivery_rate_pct'))})",
            f"Failed              : {r.get('failed')}",
            f"Opened              : {r.get('opened')} ({_fmt_rate(r.get('open_rate_pct'))})",
            f"Clicked             : {r.get('clicked')} ({_fmt_rate(r.get('click_rate_pct'))})",
            f"Click-to-open       : {_fmt_rate(r.get('ctr_engagement_pct'))}",
            "",
            "── Period ranks ──",
            f"Open rate rank      : #{r.get('rank_open_in_period')}",
            f"Click rate rank     : #{r.get('rank_click_in_period')}",
            f"Reach rank          : #{r.get('rank_reach_in_period')}",
        ]
        attributed = promo_by_cid.get(cid, 0.0)
        if attributed > 0:
            lines.append("")
            lines.append(f"Attributed revenue  : {_f_money(attributed)} (net, across all periods)")

        docs.append({
            "doc_id":       f"{org_id}_marketing_spotlight_{cid}_{_period_suffix(r.get('execution_date'))}",
            "doc_domain":   DOMAIN,
            "doc_type":     "campaign_spotlight",
            "period_start": _to_date(r.get("period")),
            "chunk_text":   "\n".join(lines),
            "metadata":     {"campaign_id": cid, "campaign_name": r["campaign_name"]},
        })
    return docs


def _build_latest_campaign_report(
    org_id: int,
    campaign_rows: list[dict],
    promo_rows: list[dict],
) -> list[dict]:
    """
    Q5, Q6, Q9, Q15 — "my last campaign" chunks.

    Produces up to 2 chunks:
      (a) most recent execution of ANY campaign (including auto-triggered SMS)
      (b) most recent execution of a NON-RECURRING / promotional campaign

    When a business owner asks "what was the open rate on my last campaign",
    they usually mean the most recent meaningful blast, not an auto-triggered
    reminder SMS. Both chunks are embedded so RAG can pick whichever is most
    semantically relevant.
    """
    executed = [r for r in campaign_rows if r.get("execution_date")]
    if not executed:
        return []

    docs = []

    # (a) Most recent execution of anything
    latest_any = max(executed, key=lambda r: r["execution_date"])
    docs.append(_latest_doc(org_id, latest_any, promo_rows, label="any", suffix="latest_any"))

    # (b) Most recent non-recurring (promotional) execution
    one_offs = [r for r in executed if not r.get("is_recurring")]
    if one_offs:
        latest_promo = max(one_offs, key=lambda r: r["execution_date"])
        # Only add if it's actually different from latest_any
        if latest_promo["campaign_id"] != latest_any["campaign_id"]:
            docs.append(_latest_doc(org_id, latest_promo, promo_rows,
                                    label="promotional", suffix="latest_promotional"))
    return docs


def _latest_doc(
    org_id: int,
    latest: dict,
    promo_rows: list[dict],
    label: str,
    suffix: str,
) -> dict:
    cid = latest["campaign_id"]
    promo_net = sum(
        float(p.get("net_revenue_after_discount") or 0)
        for p in promo_rows if p.get("campaign_id") == cid
    )

    lines = [
        f"Most recent {label} campaign report",
        "=" * 40,
        "",
        f"Campaign name       : {latest['campaign_name']}",
        f"Execution date      : {latest['execution_date']}",
        f"Channel             : {latest.get('channel')}",
        f"Recurring?          : {'Yes' if latest.get('is_recurring') else 'No (one-off)'}",
        f"Audience size       : {latest.get('audience_size')}",
        "",
        "── Performance on this run ──",
        f"Sent                : {latest.get('total_sent')}",
        f"Delivered (reach)   : {latest.get('delivered')}",
        f"Open rate           : {_fmt_rate(latest.get('open_rate_pct'))}",
        f"Click rate          : {_fmt_rate(latest.get('click_rate_pct'))}",
        f"Delivery success    : {_fmt_rate(latest.get('delivery_rate_pct'))}",
        f"Failed deliveries   : {latest.get('failed')}",
    ]
    if promo_net > 0:
        lines.append("")
        lines.append(f"Attributed revenue  : {_f_money(promo_net)}")

    return {
        "doc_id":       f"{org_id}_marketing_{suffix}",
        "doc_domain":   DOMAIN,
        "doc_type":     "latest_campaign_report",
        "period_start": _to_date(latest.get("period")),
        "chunk_text":   "\n".join(lines),
        "metadata":     {
            "campaign_id": cid,
            "execution_date": latest["execution_date"],
            "label": label,
        },
    }


def _build_roi_analysis(
    org_id: int,
    campaign_rows: list[dict],
    promo_rows: list[dict],
) -> list[dict]:
    """Q15–Q18 — did my promo campaigns pay off, and by how much."""
    if not promo_rows:
        return []

    # Aggregate per campaign
    by_campaign: dict[int, dict] = {}
    for p in promo_rows:
        cid = p["campaign_id"]
        if cid not in by_campaign:
            by_campaign[cid] = {
                "name": p.get("campaign_name") or "Campaign",
                "promo": p.get("promo_code_string") or "",
                "redemptions": 0,
                "revenue":     0.0,
                "discount":    0.0,
                "net":         0.0,
                "audience":   p.get("audience_size") or 0,
            }
        by_campaign[cid]["redemptions"] += int(p.get("redemptions") or 0)
        by_campaign[cid]["revenue"]     += float(p.get("attributed_revenue") or 0)
        by_campaign[cid]["discount"]    += float(p.get("total_discount_given") or 0)
        by_campaign[cid]["net"]         += float(p.get("net_revenue_after_discount") or 0)

    lines = [
        "Campaign ROI summary",
        "=" * 20,
        "",
        "Revenue attribution across all tracked periods:",
        "",
    ]

    total_net = sum(c["net"] for c in by_campaign.values())
    total_disc = sum(c["discount"] for c in by_campaign.values())
    total_red = sum(c["redemptions"] for c in by_campaign.values())
    lines.append(f"Total redemptions   : {total_red}")
    lines.append(f"Total net revenue   : {_f_money(total_net)}")
    lines.append(f"Total discount given: {_f_money(total_disc)}")
    lines.append("")
    lines.append("Per campaign:")

    for c in sorted(by_campaign.values(), key=lambda x: -x["net"]):
        conv = _safe_pct(c["redemptions"], c["audience"] or 0, decimals=2)
        paid_off = "Yes" if c["net"] > 0 else "No"
        revenue_per_send = (
            f"{c['net']/c['audience']:.4f}" if c["audience"] else "N/A"
        )
        lines.append(
            f"  • {c['name']} (promo={c['promo']}) — "
            f"{c['redemptions']} redemptions, "
            f"audience {c['audience']}, "
            f"conversion {conv}, "
            f"net revenue {_f_money(c['net'])}, "
            f"revenue-per-send ${revenue_per_send}, paid off: {paid_off}"
        )

    return [{
        "doc_id":       f"{org_id}_marketing_roi_analysis",
        "doc_domain":   DOMAIN,
        "doc_type":     "roi_analysis",
        "period_start": None,
        "chunk_text":   "\n".join(lines),
        "metadata":     {"campaign_count": len(by_campaign), "total_net_revenue": total_net},
    }]


# ═════════════════════════════════════════════════════════════════════════════
# Helpers — embedding / storage
# ═════════════════════════════════════════════════════════════════════════════

async def _embed_and_upsert(
    tenant: str,
    embedding_client: Any,
    vector_store: Any,
    doc: dict,
    force: bool,
) -> str:
    """Returns 'created', 'skipped', or 'failed'."""
    chunk_text = doc["chunk_text"]
    content_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
    meta = dict(doc.get("metadata") or {})

    if not force:
        try:
            existing = await vector_store.get_doc_ids(
                tenant, doc["doc_domain"], doc["doc_type"],
            )
            if doc["doc_id"] in existing:
                stored = await vector_store.get_doc_metadata(
                    tenant, doc["doc_id"],
                ) or {}
                if stored.get("content_hash") == content_hash:
                    return "skipped"
        except Exception:
            logger.debug(
                "marketing_docs: existing-doc check failed; will re-embed",
                exc_info=True,
            )

    meta["content_hash"] = content_hash
    try:
        vec = await embedding_client.embed(chunk_text)
        await vector_store.upsert(
            tenant_id=tenant,
            doc_id=doc["doc_id"],
            doc_domain=doc["doc_domain"],
            doc_type=doc["doc_type"],
            chunk_text=chunk_text,
            embedding=vec,
            period_start=doc.get("period_start"),
            metadata=meta,
        )
    except Exception:
        logger.exception("marketing_docs: embed/upsert failed doc_id=%s", doc["doc_id"])
        return "failed"
    return "created"


# ═════════════════════════════════════════════════════════════════════════════
# Formatting helpers
# ═════════════════════════════════════════════════════════════════════════════

def _period_label(period: str) -> str:
    try:
        d = date.fromisoformat(str(period)[:10])
        return d.strftime("%B %Y")
    except (ValueError, TypeError):
        return str(period)

def _period_suffix(period) -> str:
    if not period:
        return "null"
    try:
        d = date.fromisoformat(str(period)[:10])
        return d.strftime("%Y_%m")
    except (ValueError, TypeError):
        return str(period).replace("-", "_").replace(":", "_")[:20]

def _to_date(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None

def _f_money(x) -> str:
    try:
        v = float(x or 0)
    except (TypeError, ValueError):
        return "$0"
    return f"${v:,.2f}"

def _fmt_rate(x) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):.2f}%"
    except (TypeError, ValueError):
        return "N/A"

def _fmt_rate_signed(x) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):+.2f}%"
    except (TypeError, ValueError):
        return "N/A"

def _fmt_int(x) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{int(x):,}"
    except (TypeError, ValueError):
        return "N/A"

def _fmt_int_signed(x) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{int(x):+d}"
    except (TypeError, ValueError):
        return "N/A"

def _safe_pct(part, total, decimals: int = 0) -> str:
    try:
        part = float(part or 0)
        total = float(total or 0)
    except (TypeError, ValueError):
        return "N/A"
    if total <= 0:
        return "N/A"
    return f"{part / total * 100:.{decimals}f}%"

def _pad(x, width: int) -> str:
    s = _fmt_int(x)
    return s.rjust(width)

def _pad_signed(x, width: int) -> str:
    s = _fmt_int_signed(x)
    return s.rjust(width)