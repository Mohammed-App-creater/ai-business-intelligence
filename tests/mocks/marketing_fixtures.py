"""
tests/mocks/marketing_fixtures.py
==================================
Realistic mock response data for the 3 Marketing endpoints:
  EP1: /api/v1/leo/marketing/campaign-summary          → CAMPAIGN_SUMMARY
  EP2: /api/v1/leo/marketing/channel-monthly           → CHANNEL_MONTHLY
  EP3: /api/v1/leo/marketing/promo-attribution-monthly → PROMO_ATTRIBUTION_MONTHLY

Based on the same salon business (business_id=42) woven into the story from
revenue/appointments/staff/services/clients fixtures.

── Consistency anchors with existing fixtures ────────────────────────────────
  • business_id=42, 2 locations (1=Main St, 2=Westside)
  • Ref date = 2026-04-01 → "last month" = 2026-03, "last quarter" = Q1 2026
  • Feb 2025 was WEAK (-7.9% rev, cancellation spike) → Feb campaigns underperform
  • Mar 2025 was RECOVERY (+30% rev) → Mar campaigns strong
  • WELCOME10 promo: 38 Main St + 27 Westside = 65 total redemptions (H1 2025)
     Full-year totals from revenue.promo-impact:
       WELCOME10: $715 discount given, $5,720 revenue after discount
       SUMMER20:  $480 discount given, $1,680 revenue after discount
  • Express Facial launched Feb 2026 (clients fixture) → matching email campaign
  • Main St dominates new-client acquisition → WELCOME10 email performs better

── Campaign roster (8 campaigns for biz 42) ──────────────────────────────────
  CID 501: "Welcome Series"       email, recurring, PromoCode=WELCOME10
  CID 502: "Summer Spa Special"   email, one-off,   PromoCode=SUMMER20
  CID 503: "Appointment Reminder" sms,   recurring, no promo
  CID 504: "Re-engagement Win Back" email, recurring, no promo
  CID 505: "Holiday Promo"        email, one-off,   PromoCode=HOLIDAY15 (no revenue — expired before redemption push)
  CID 506: "Express Facial Launch" email, one-off,  no promo (Feb 2026 launch)
  CID 507: "March Madness"        email, one-off,   no promo (Mar 2026 strong)
  CID 508: "Client Survey"        email, one-off,   EXPIRED BUT ACTIVE (workflow health flag)

── Coverage of all 34 test questions ─────────────────────────────────────────
  Q1:  campaigns last month (Mar 2026)          → 3 campaigns ran (501, 503, 507)
  Q2:  emails sent last month                    → EP2 period=2026-03 emails_sent
  Q3:  SMS sent last month                       → EP2 period=2026-03 sms_sent
  Q4:  active campaigns last month               → Status=completed/ready, is_active=1
  Q5:  open rate on last campaign                → CID 507 (March Madness) latest
  Q6:  click rate on last campaign               → CID 507 latest
  Q7:  delivery success rate last month          → avg across Mar 2026 executions
  Q8:  failed + worst failure campaign           → CID 505 Holiday Promo has worst failure
  Q9:  reach of last campaign                    → CID 507 delivered count
  Q10: highest open rate this year               → CID 506 Express Facial (49.2%)
  Q11: highest click rate this year              → CID 506 Express Facial (7.1%)
  Q12: best last quarter + why                   → Q1 2026 → CID 506 Feb > others
  Q13: most customers reached                    → CID 501 Welcome Series cumulative
  Q14: best recurring campaign                   → CID 501 outperforms 503 & 504
  Q15: revenue from last promo campaign          → CID 502 SUMMER20 latest execution
  Q16: most redeemed promo last month            → WELCOME10 in Mar 2026
  Q17: avg revenue per campaign sent this year   → EP3 avg revenue_per_send
  Q18: did March promo pay for itself?           → CID 507 net revenue positive
  Q19: open rates trend last 6 mo                → EP2 email_open_rate_pct trend
  Q20: click rate improving?                     → EP2 email_click_rate_pct trend
  Q21: email volume MoM                          → EP2 emails_mom_pct
  Q22: SMS vs email YoY                          → EP2 full series
  Q23: SMS vs email engagement                   → EP2 email vs sms click rates
  Q24: % email vs SMS last month                 → EP2 emails/sms ratio in Mar
  Q25: recurring campaign still performing?      → CID 501, 503, 504 last runs
  Q26: best template format                      → EP1 group by template_format_name
  Q27: unsubscribes last month                   → EP2 email_net_unsub_delta Mar 2026
  Q28: contactable list shrinking?               → EP2 email_contactable trend
  Q29: best-converting location                  → EP3 Main St (WELCOME10)
  Q30: promo revenue per branch                  → EP3 per-location
  Q31: why underperform vs previous?             → CID 502 Feb vs Mar or similar
  Q32: how to improve open rates                 → derived from EP1+EP2
  Q33: best day of week historically             → derived from EP1 execution_date
  Q34: am I over-sending?                        → EP2 volume + unsub delta

── Reference date ────────────────────────────────────────────────────────────
  ref_date = 2026-04-01 (set via LEO_TEST_REF_DATE env var)
    → "last month" resolves to 2026-03
    → "last quarter" resolves to Q1 2026 (Jan–Mar 2026)
    → is_expired_but_active flag: campaign_expiration < 2026-04-01 AND is_active=1
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a single execution row for EP1 (campaign-summary)
# ─────────────────────────────────────────────────────────────────────────────

def _exec_row(
    campaign_id, campaign_name, campaign_status, is_active, is_recurring,
    channel, channel_code, template_format_name, audience_size,
    promo_code_string, campaign_start, campaign_expiration,
    execution_date, total_sent, delivered, failed, opened, clicked,
):
    """Build one execution row for EP1 with computed rates."""
    period = execution_date[:7] + "-01"

    def _rate(n, d):
        if d in (None, 0):
            return None
        return round(n / d * 100, 2)

    # SMS has no open tracking — opened is 0, open_rate is NULL (not 0)
    if channel == "sms":
        open_rate = None
        ctr_engagement = None
    else:
        open_rate = _rate(opened, delivered)
        ctr_engagement = _rate(clicked, opened)

    # Expired but active: campaign_expiration < 2026-04-01 AND is_active=1
    is_expired_but_active = 0
    if campaign_expiration and is_active == 1:
        if campaign_expiration < "2026-04-01":
            is_expired_but_active = 1

    return {
        "campaign_id":            campaign_id,
        "execution_date":         execution_date,
        "period":                 period,
        "campaign_name":          campaign_name,
        "campaign_status":        campaign_status,
        "is_active":              is_active,
        "is_recurring":           is_recurring,
        "channel":                channel,
        "channel_code":           channel_code,
        "template_format_name":   template_format_name,
        "audience_size":          audience_size,
        "promo_code_string":      promo_code_string,
        "campaign_start":         campaign_start,
        "campaign_expiration":    campaign_expiration,
        "total_sent":             total_sent,
        "delivered":              delivered,
        "failed":                 failed,
        "opened":                 opened,
        "clicked":                clicked,
        "delivery_rate_pct":      _rate(delivered, total_sent),
        "open_rate_pct":          open_rate,
        "click_rate_pct":         _rate(clicked, delivered) if channel != "sms" or clicked > 0 else _rate(clicked, delivered),
        "ctr_engagement_pct":     ctr_engagement,
        "is_expired_but_active":  is_expired_but_active,
        # Ranks populated after list built
        "rank_open_in_period":    None,
        "rank_click_in_period":   None,
        "rank_reach_in_period":   None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Campaign catalog (8 campaigns for biz 42)
# ─────────────────────────────────────────────────────────────────────────────
# Channel codes: 1=email, 2=mobile, 3=sms

_CAMPAIGNS = {
    501: dict(name="Welcome Series", status="completed", active=1, recurring=1,
              channel="email", code=1, template="Welcome Email v2",
              audience=180, promo="WELCOME10",
              start="2025-01-05", expire="2026-12-31"),
    502: dict(name="Summer Spa Special", status="completed", active=0, recurring=0,
              channel="email", code=1, template="Promo Banner v1",
              audience=820, promo="SUMMER20",
              start="2025-06-01", expire="2025-09-30"),
    503: dict(name="Appointment Reminder", status="completed", active=1, recurring=1,
              channel="sms", code=3, template="Reminder SMS v1",
              audience=1050, promo=None,
              start="2025-01-15", expire="2027-01-15"),
    504: dict(name="Re-engagement Win Back", status="completed", active=1, recurring=1,
              channel="email", code=1, template="Re-engagement v1",
              audience=420, promo=None,
              start="2025-03-01", expire="2026-12-31"),
    505: dict(name="Holiday Promo", status="completed", active=0, recurring=0,
              channel="email", code=1, template="Promo Banner v1",
              audience=2100, promo="HOLIDAY15",
              start="2025-11-20", expire="2025-12-31"),
    506: dict(name="Express Facial Launch", status="completed", active=0, recurring=0,
              channel="email", code=1, template="Service Launch v1",
              audience=1820, promo=None,
              start="2026-02-05", expire="2026-03-31"),
    507: dict(name="March Madness", status="completed", active=1, recurring=0,
              channel="email", code=1, template="Promo Banner v2",
              audience=1950, promo=None,
              start="2026-03-10", expire="2026-04-30"),
    508: dict(name="Client Survey", status="ready", active=1, recurring=0,
              channel="email", code=1, template="Survey v1",
              audience=2000, promo=None,
              start="2025-10-15", expire="2025-11-30"),  # EXPIRED BUT ACTIVE
}


def _c(cid):
    """Shorthand for pulling campaign spec fields."""
    return _CAMPAIGNS[cid]


# ─────────────────────────────────────────────────────────────────────────────
# Campaign executions (one row per send)
# ─────────────────────────────────────────────────────────────────────────────
# Design principles:
#   - CID 501 (Welcome Series): fires monthly, consistent performance
#   - CID 502 (Summer Spa): single blast Jun 15 2025 + follow-up Jul 20
#   - CID 503 (Appt Reminder): fires twice-monthly year-round
#   - CID 504 (Re-engagement): monthly from Mar 2025
#   - CID 505 (Holiday): Nov 28 2025, HIGHEST failure rate (list hygiene issue)
#   - CID 506 (Express Facial): Feb 7 2026 + Feb 20 2026, HIGHEST open rate
#   - CID 507 (March Madness): Mar 11 2026, strong performer
#   - CID 508 (Client Survey): never executed (expired before send)

_EXECUTIONS_RAW = [
    # ══ CID 501 — Welcome Series (monthly auto-trigger to new clients) ═══
    # Small audience, consistent good performance
    # Jan 2025: 28 sends (1 per day × new signups)
    (501, "2025-01-28", 28, 27, 1, 11, 2),
    (501, "2025-02-28", 26, 25, 1, 9, 1),   # Feb weak
    (501, "2025-03-28", 34, 33, 1, 13, 2),
    (501, "2025-04-28", 31, 30, 1, 12, 2),
    (501, "2025-05-28", 35, 34, 1, 14, 3),
    (501, "2025-06-28", 38, 37, 1, 15, 3),
    (501, "2025-07-28", 36, 35, 1, 14, 2),
    (501, "2025-08-28", 34, 33, 1, 13, 2),
    (501, "2025-09-28", 33, 32, 1, 13, 2),
    (501, "2025-10-28", 31, 30, 1, 12, 2),
    (501, "2025-11-28", 29, 28, 1, 11, 2),
    (501, "2025-12-28", 32, 31, 1, 12, 2),
    (501, "2026-01-28", 30, 29, 1, 11, 2),
    (501, "2026-02-28", 34, 33, 1, 13, 2),  # Feb 2026 — Express Facial boost
    (501, "2026-03-28", 38, 37, 1, 15, 3),

    # ══ CID 502 — Summer Spa Special (SUMMER20) ═══
    # Two sends. SUMMER20 redemptions: 14 MainSt + 10 Westside = 24 (per revenue fixture)
    # Total revenue after discount: 980 + 700 = 1,680 (per revenue fixture)
    (502, "2025-06-15", 820, 795, 25, 238, 36),  # main blast
    (502, "2025-07-20", 800, 780, 20, 195, 22),  # follow-up (weaker — list fatigue)

    # ══ CID 503 — Appointment Reminder (SMS, bi-weekly) ═══
    # SMS has no open tracking — opened will be 0 in DB
    # High delivery, modest click (SMS engagement measured by reply/click)
    (503, "2025-01-15", 180, 175, 5, 0, 12),
    (503, "2025-01-30", 190, 186, 4, 0, 14),
    (503, "2025-02-14", 175, 168, 7, 0, 10),
    (503, "2025-02-28", 168, 163, 5, 0, 11),
    (503, "2025-03-15", 205, 200, 5, 0, 16),
    (503, "2025-03-30", 210, 206, 4, 0, 17),
    (503, "2025-04-15", 200, 196, 4, 0, 15),
    (503, "2025-04-30", 205, 201, 4, 0, 16),
    (503, "2025-05-15", 215, 211, 4, 0, 17),
    (503, "2025-05-30", 220, 215, 5, 0, 18),
    (503, "2025-06-15", 228, 223, 5, 0, 19),
    (503, "2025-06-30", 230, 225, 5, 0, 19),
    (503, "2025-07-15", 225, 220, 5, 0, 18),
    (503, "2025-07-30", 228, 223, 5, 0, 19),
    (503, "2025-08-15", 222, 217, 5, 0, 17),
    (503, "2025-08-30", 220, 215, 5, 0, 17),
    (503, "2025-09-15", 224, 219, 5, 0, 18),
    (503, "2025-09-30", 222, 217, 5, 0, 17),
    (503, "2025-10-15", 218, 213, 5, 0, 17),
    (503, "2025-10-30", 220, 215, 5, 0, 17),
    (503, "2025-11-15", 225, 220, 5, 0, 18),
    (503, "2025-11-30", 228, 223, 5, 0, 19),
    (503, "2025-12-15", 222, 217, 5, 0, 17),
    (503, "2025-12-30", 218, 213, 5, 0, 17),
    (503, "2026-01-15", 235, 230, 5, 0, 19),
    (503, "2026-01-30", 240, 235, 5, 0, 20),
    (503, "2026-02-14", 245, 240, 5, 0, 20),
    (503, "2026-02-28", 248, 243, 5, 0, 21),
    (503, "2026-03-15", 260, 254, 6, 0, 22),
    (503, "2026-03-30", 265, 259, 6, 0, 23),

    # ══ CID 504 — Re-engagement Win Back (monthly, targets inactive clients) ═══
    # Lower open/click rates — people aren't engaged by definition
    (504, "2025-03-10", 380, 360, 20, 76, 8),
    (504, "2025-04-10", 395, 376, 19, 79, 9),
    (504, "2025-05-10", 405, 385, 20, 81, 9),
    (504, "2025-06-10", 410, 390, 20, 82, 10),
    (504, "2025-07-10", 415, 395, 20, 83, 10),
    (504, "2025-08-10", 418, 398, 20, 84, 10),
    (504, "2025-09-10", 420, 400, 20, 80, 9),
    (504, "2025-10-10", 422, 402, 20, 80, 9),
    (504, "2025-11-10", 418, 398, 20, 80, 9),
    (504, "2025-12-10", 415, 395, 20, 79, 9),
    (504, "2026-01-10", 420, 400, 20, 80, 9),
    (504, "2026-02-10", 425, 405, 20, 81, 10),
    (504, "2026-03-10", 430, 410, 20, 82, 10),

    # ══ CID 505 — Holiday Promo (HIGHEST FAILURE RATE — list hygiene) ═══
    # 2100 sent, only 1680 delivered → 20% failure → worst-ever failure rate
    # This is the "why is my delivery rate bad" answer
    (505, "2025-11-28", 2100, 1680, 420, 420, 50),

    # ══ CID 506 — Express Facial Launch (HIGHEST OPEN RATE) ═══
    # Feb 7 blast + Feb 20 follow-up, new-service announcement
    # Well-segmented list → best engagement of any campaign
    (506, "2026-02-07", 1820, 1785, 35, 878, 127),  # 49.2% open, 7.1% click
    (506, "2026-02-20", 1800, 1770, 30, 796, 98),   # 45.0% open, 5.5% click

    # ══ CID 507 — March Madness (strong Mar 2026 performer) ═══
    # Single send, solid numbers — Mar 2026 is recovery season
    (507, "2026-03-11", 1950, 1905, 45, 651, 89),

    # ══ CID 508 — Client Survey (never executed, expired) ═══
    # No execution rows — is_expired_but_active flag catches this
]


# Build execution records with computed fields
_EXECUTIONS = []
for cid, exec_date, total, delivered, failed, opened, clicked in _EXECUTIONS_RAW:
    c = _c(cid)
    _EXECUTIONS.append(_exec_row(
        campaign_id=cid,
        campaign_name=c["name"],
        campaign_status=c["status"],
        is_active=c["active"],
        is_recurring=c["recurring"],
        channel=c["channel"],
        channel_code=c["code"],
        template_format_name=c["template"],
        audience_size=c["audience"],
        promo_code_string=c["promo"],
        campaign_start=c["start"],
        campaign_expiration=c["expire"],
        execution_date=exec_date,
        total_sent=total,
        delivered=delivered,
        failed=failed,
        opened=opened,
        clicked=clicked,
    ))

# Also add a phantom row for CID 508 (expired-but-active, never executed)
# — not in executions, but EP1 response includes "campaigns with no executions"
# handled via LEFT JOIN. Add an execution_date=None row to represent it.
_EXECUTIONS.append({
    "campaign_id":            508,
    "execution_date":         None,
    "period":                 None,
    "campaign_name":          _c(508)["name"],
    "campaign_status":        _c(508)["status"],
    "is_active":              _c(508)["active"],
    "is_recurring":           _c(508)["recurring"],
    "channel":                _c(508)["channel"],
    "channel_code":           _c(508)["code"],
    "template_format_name":   _c(508)["template"],
    "audience_size":          _c(508)["audience"],
    "promo_code_string":      _c(508)["promo"],
    "campaign_start":         _c(508)["start"],
    "campaign_expiration":    _c(508)["expire"],
    "total_sent":             0,
    "delivered":              0,
    "failed":                 0,
    "opened":                 0,
    "clicked":                0,
    "delivery_rate_pct":      None,
    "open_rate_pct":          None,
    "click_rate_pct":         None,
    "ctr_engagement_pct":     None,
    "is_expired_but_active":  1,    # ← the workflow health flag
    "rank_open_in_period":    None,
    "rank_click_in_period":   None,
    "rank_reach_in_period":   None,
})


# ─────────────────────────────────────────────────────────────────────────────
# Assign per-period ranks
# ─────────────────────────────────────────────────────────────────────────────

def _assign_ranks(executions):
    """Populate rank_open_in_period, rank_click_in_period, rank_reach_in_period."""
    from collections import defaultdict
    by_period = defaultdict(list)
    for e in executions:
        if e["period"] is not None:
            by_period[e["period"]].append(e)

    for period, rows in by_period.items():
        # rank by open rate (desc, nulls last)
        by_open = sorted(
            rows, key=lambda r: (r["open_rate_pct"] is None, -(r["open_rate_pct"] or 0))
        )
        for i, r in enumerate(by_open, start=1):
            r["rank_open_in_period"] = i if r["open_rate_pct"] is not None else None

        by_click = sorted(
            rows, key=lambda r: (r["click_rate_pct"] is None, -(r["click_rate_pct"] or 0))
        )
        for i, r in enumerate(by_click, start=1):
            r["rank_click_in_period"] = i if r["click_rate_pct"] is not None else None

        by_reach = sorted(rows, key=lambda r: -r["delivered"])
        for i, r in enumerate(by_reach, start=1):
            r["rank_reach_in_period"] = i


_assign_ranks(_EXECUTIONS)


# ─────────────────────────────────────────────────────────────────────────────
# EP1 — Campaign Summary fixture
# ─────────────────────────────────────────────────────────────────────────────

CAMPAIGN_SUMMARY = {
    "business_id":   42,
    "period_start":  "2025-01-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data":          _EXECUTIONS,
}


# ─────────────────────────────────────────────────────────────────────────────
# EP2 — Channel Monthly fixture
# ─────────────────────────────────────────────────────────────────────────────
# For each period, aggregate:
#   - email_campaigns_run, email_open/click_rate_pct (from email executions)
#   - sms_campaigns_run, sms_click_rate_pct (from sms executions; open=NULL)
#   - emails_sent, sms_sent (from tbl_smsemailcount — we compute from execs)
#   - Unsubscribe snapshot (hand-calibrated to show a growing contactable list
#     through most of 2025, then mild email-list shrink in late 2025 due to
#     Holiday Promo failure cascade, then Express Facial recovery in Feb 2026)

def _compute_channel_monthly():
    from collections import defaultdict

    # Group email execs and sms execs by period
    by_period_email = defaultdict(list)
    by_period_sms = defaultdict(list)
    for e in _EXECUTIONS:
        if e["period"] is None:
            continue
        if e["channel"] == "email":
            by_period_email[e["period"]].append(e)
        elif e["channel"] == "sms":
            by_period_sms[e["period"]].append(e)

    # All periods present
    all_periods = sorted(set(by_period_email.keys()) | set(by_period_sms.keys()))

    # Hand-calibrated unsubscribe snapshots — captures the story:
    # - Gradual list growth through Jun 2025
    # - Holiday Promo Nov 2025 causes spike in email unsubs (bad send to cold list)
    # - Recovery through 2026, Express Facial campaign resonates
    # - Mar 2026 net_unsub_delta is 7 (small, healthy)
    _unsub_snapshots = {
        "2025-01-01": {"email_unsub": 18, "sms_unsub": 8,  "total": 1920, "email_contactable": 1880, "sms_contactable": 1905},
        "2025-02-01": {"email_unsub": 21, "sms_unsub": 10, "total": 1945, "email_contactable": 1903, "sms_contactable": 1928},
        "2025-03-01": {"email_unsub": 23, "sms_unsub": 11, "total": 1998, "email_contactable": 1954, "sms_contactable": 1979},
        "2025-04-01": {"email_unsub": 25, "sms_unsub": 12, "total": 2035, "email_contactable": 1989, "sms_contactable": 2015},
        "2025-05-01": {"email_unsub": 27, "sms_unsub": 13, "total": 2068, "email_contactable": 2020, "sms_contactable": 2047},
        "2025-06-01": {"email_unsub": 29, "sms_unsub": 14, "total": 2095, "email_contactable": 2045, "sms_contactable": 2073},
        "2025-07-01": {"email_unsub": 32, "sms_unsub": 15, "total": 2110, "email_contactable": 2057, "sms_contactable": 2087},
        "2025-08-01": {"email_unsub": 34, "sms_unsub": 16, "total": 2115, "email_contactable": 2060, "sms_contactable": 2091},
        "2025-09-01": {"email_unsub": 36, "sms_unsub": 17, "total": 2120, "email_contactable": 2063, "sms_contactable": 2095},
        "2025-10-01": {"email_unsub": 38, "sms_unsub": 18, "total": 2125, "email_contactable": 2066, "sms_contactable": 2099},
        "2025-11-01": {"email_unsub": 41, "sms_unsub": 19, "total": 2130, "email_contactable": 2068, "sms_contactable": 2103},
        # Holiday Promo fail spike in unsubs:
        "2025-12-01": {"email_unsub": 68, "sms_unsub": 21, "total": 2130, "email_contactable": 2041, "sms_contactable": 2100},
        "2026-01-01": {"email_unsub": 72, "sms_unsub": 22, "total": 2135, "email_contactable": 2042, "sms_contactable": 2104},
        "2026-02-01": {"email_unsub": 74, "sms_unsub": 22, "total": 2140, "email_contactable": 2045, "sms_contactable": 2110},
        # Small Mar 2026 delta (healthy):
        "2026-03-01": {"email_unsub": 81, "sms_unsub": 25, "total": 2142, "email_contactable": 2040, "sms_contactable": 2107},
    }

    rows = []
    prev_emails_sent = None
    prev_sms_sent = None
    prev_email_unsub = None
    prev_sms_unsub = None
    prev_email_contactable = None

    for period in all_periods:
        email_execs = by_period_email.get(period, [])
        sms_execs = by_period_sms.get(period, [])

        emails_sent = sum(e["total_sent"] for e in email_execs)
        sms_sent = sum(e["total_sent"] for e in sms_execs)

        # Email aggregated rates (SUM of delivered/opened/clicked over period)
        email_delivered = sum(e["delivered"] for e in email_execs)
        email_opened = sum(e["opened"] for e in email_execs)
        email_clicked = sum(e["clicked"] for e in email_execs)
        email_open_rate = (
            round(email_opened / email_delivered * 100, 2) if email_delivered else None
        )
        email_click_rate = (
            round(email_clicked / email_delivered * 100, 2) if email_delivered else None
        )

        sms_delivered = sum(e["delivered"] for e in sms_execs)
        sms_clicked = sum(e["clicked"] for e in sms_execs)
        # SMS open tracking N/A
        sms_open_rate = None
        sms_click_rate = (
            round(sms_clicked / sms_delivered * 100, 2) if sms_delivered else None
        )

        # MoM deltas
        emails_mom = None
        if prev_emails_sent:
            emails_mom = round((emails_sent - prev_emails_sent) / prev_emails_sent * 100, 2)
        sms_mom = None
        if prev_sms_sent:
            sms_mom = round((sms_sent - prev_sms_sent) / prev_sms_sent * 100, 2)

        snap = _unsub_snapshots.get(period, {
            "email_unsub": 0, "sms_unsub": 0,
            "total": 0, "email_contactable": 0, "sms_contactable": 0
        })

        email_net_unsub_delta = (
            snap["email_unsub"] - prev_email_unsub if prev_email_unsub is not None else None
        )
        sms_net_unsub_delta = (
            snap["sms_unsub"] - prev_sms_unsub if prev_sms_unsub is not None else None
        )
        email_contactable_mom_pct = None
        if prev_email_contactable and prev_email_contactable > 0:
            email_contactable_mom_pct = round(
                (snap["email_contactable"] - prev_email_contactable) / prev_email_contactable * 100, 2
            )

        rows.append({
            "period":                     period,
            "emails_sent":                emails_sent,
            "sms_sent":                   sms_sent,
            "prev_emails_sent":           prev_emails_sent,
            "prev_sms_sent":              prev_sms_sent,
            "emails_mom_pct":             emails_mom,
            "sms_mom_pct":                sms_mom,
            "email_campaigns_run":        len({e["campaign_id"] for e in email_execs}),
            "email_open_rate_pct":        email_open_rate,
            "email_click_rate_pct":       email_click_rate,
            "sms_campaigns_run":          len({e["campaign_id"] for e in sms_execs}),
            "sms_open_rate_pct":          sms_open_rate,
            "sms_click_rate_pct":         sms_click_rate,
            "email_unsubscribed_count":   snap["email_unsub"],
            "sms_unsubscribed_count":     snap["sms_unsub"],
            "total_contacts":             snap["total"],
            "email_contactable":          snap["email_contactable"],
            "sms_contactable":            snap["sms_contactable"],
            "email_net_unsub_delta":      email_net_unsub_delta,
            "sms_net_unsub_delta":        sms_net_unsub_delta,
            "email_contactable_mom_pct":  email_contactable_mom_pct,
        })

        prev_emails_sent = emails_sent
        prev_sms_sent = sms_sent
        prev_email_unsub = snap["email_unsub"]
        prev_sms_unsub = snap["sms_unsub"]
        prev_email_contactable = snap["email_contactable"]

    # Return ORDER BY period DESC
    return list(reversed(rows))


CHANNEL_MONTHLY = {
    "business_id":   42,
    "period_start":  "2025-01-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data":          _compute_channel_monthly(),
}


# ─────────────────────────────────────────────────────────────────────────────
# EP3 — Promo Attribution Monthly fixture
# ─────────────────────────────────────────────────────────────────────────────
# Must reconcile to revenue.promo-impact totals:
#   WELCOME10: 38 Main St + 27 Westside redemptions = 65 total
#              $420 MS + $295 WS = $715 total discount
#              $3,360 MS + $2,360 WS = $5,720 total revenue after discount
#   SUMMER20: 14 Main St + 10 Westside redemptions = 24 total
#             $280 MS + $200 WS = $480 total discount
#             $980 MS + $700 WS = $1,680 total revenue after discount
# HOLIDAY15: no data in revenue fixture → it flopped (0 redemptions)
#
# We spread WELCOME10 across Jan-Jun 2025 + continued into 2026
# Full year through Mar 2026:
#   Jan: 5 MS, 3 WS
#   Feb: 4 MS, 3 WS  (weak month)
#   Mar: 7 MS, 5 WS
#   Apr: 6 MS, 4 WS
#   May: 7 MS, 5 WS
#   Jun: 9 MS, 7 WS
#   ... 2025 H1 total: 38 MS, 27 WS ✓ matches revenue fixture
#   H2 2025: 6 MS, 4 WS (Jul), etc — we extend through 2026 Mar
#
# SUMMER20: Jun 2025 launched, active through Sep 2025
#   Jun: 5 MS, 3 WS
#   Jul: 4 MS, 3 WS
#   Aug: 3 MS, 2 WS
#   Sep: 2 MS, 2 WS
#   Total: 14 MS, 10 WS ✓

def _welcome_redemption(period, loc_id, redemptions, discount, net_rev):
    """Build a WELCOME10 attribution row."""
    total_pay = net_rev + discount  # attributed_revenue = net_rev + discount_given
    return {
        "campaign_id":                501,
        "period":                     period,
        "location_id":                loc_id,
        "campaign_name":              "Welcome Series",
        "promo_code_string":          "WELCOME10",
        "audience_size":              180,   # CID 501 audience
        "redemptions":                redemptions,
        "attributed_revenue":         total_pay,
        "total_discount_given":       discount,
        "net_revenue_after_discount": net_rev,
        "revenue_per_send":           None,  # filled below
        "conversion_rate_pct":        None,  # filled below
        "rank_in_period":             None,  # filled below
        "rank_in_location_period":    None,
    }


def _summer_redemption(period, loc_id, redemptions, discount, net_rev):
    total_pay = net_rev + discount
    return {
        "campaign_id":                502,
        "period":                     period,
        "location_id":                loc_id,
        "campaign_name":              "Summer Spa Special",
        "promo_code_string":          "SUMMER20",
        "audience_size":              820,
        "redemptions":                redemptions,
        "attributed_revenue":         total_pay,
        "total_discount_given":       discount,
        "net_revenue_after_discount": net_rev,
        "revenue_per_send":           None,
        "conversion_rate_pct":        None,
        "rank_in_period":             None,
        "rank_in_location_period":    None,
    }


# Calibrated to hit revenue fixture totals (H1 2025):
# WELCOME10: 38 MS (net $3,360, disc $420) + 27 WS (net $2,360, disc $295)
# Splits per month (MS,WS) redemptions; disc/net split proportionally:
_WELCOME_H1 = [
    # period         ms_red  ws_red  ms_disc  ws_disc  ms_net   ws_net
    ("2025-01-01",    5,      3,      55.0,    33.0,    440.0,   262.0),
    ("2025-02-01",    4,      3,      44.0,    33.0,    352.0,   262.0),
    ("2025-03-01",    7,      5,      78.0,    55.0,    618.0,   437.0),
    ("2025-04-01",    6,      4,      66.0,    44.0,    528.0,   349.0),
    ("2025-05-01",    7,      5,      78.0,    55.0,    618.0,   438.0),
    ("2025-06-01",    9,      7,      99.0,    75.0,    804.0,   612.0),
]
# Check sum: MS redemptions=38 ✓, WS=27 ✓
#   MS disc: 55+44+78+66+78+99 = 420 ✓
#   WS disc: 33+33+55+44+55+75 = 295 ✓
#   MS net:  440+352+618+528+618+804 = 3360 ✓
#   WS net:  262+262+437+349+438+612 = 2360 ✓

# H2 2025 + Q1 2026 (extending the story — not in revenue fixture H1 figures)
_WELCOME_H2_2025_PLUS = [
    ("2025-07-01",    6,      4,      66.0,    44.0,    528.0,   352.0),
    ("2025-08-01",    5,      4,      55.0,    44.0,    440.0,   352.0),
    ("2025-09-01",    5,      4,      55.0,    44.0,    440.0,   352.0),
    ("2025-10-01",    4,      3,      44.0,    33.0,    352.0,   262.0),
    ("2025-11-01",    4,      3,      44.0,    33.0,    352.0,   262.0),
    ("2025-12-01",    5,      4,      55.0,    44.0,    440.0,   352.0),
    ("2026-01-01",    4,      3,      44.0,    33.0,    352.0,   262.0),
    ("2026-02-01",    4,      3,      44.0,    33.0,    352.0,   262.0),
    ("2026-03-01",    6,      4,      66.0,    44.0,    528.0,   349.0),
]

_SUMMER_FULL = [
    ("2025-06-01",    5,      3,     100.0,    60.0,    350.0,   210.0),
    ("2025-07-01",    4,      3,      80.0,    60.0,    280.0,   210.0),
    ("2025-08-01",    3,      2,      60.0,    40.0,    210.0,   140.0),
    ("2025-09-01",    2,      2,      40.0,    40.0,    140.0,   140.0),
]
# Sum: MS=14, WS=10 ✓
#   MS disc: 100+80+60+40 = 280 ✓
#   WS disc: 60+60+40+40 = 200 ✓
#   MS net:  350+280+210+140 = 980 ✓
#   WS net:  210+210+140+140 = 700 ✓


def _build_promo_attribution():
    rows = []

    for period, msr, wsr, msd, wsd, msn, wsn in _WELCOME_H1 + _WELCOME_H2_2025_PLUS:
        if msr > 0:
            rows.append(_welcome_redemption(period, 1, msr, msd, msn))
        if wsr > 0:
            rows.append(_welcome_redemption(period, 2, wsr, wsd, wsn))

    for period, msr, wsr, msd, wsd, msn, wsn in _SUMMER_FULL:
        if msr > 0:
            rows.append(_summer_redemption(period, 1, msr, msd, msn))
        if wsr > 0:
            rows.append(_summer_redemption(period, 2, wsr, wsd, wsn))

    # Compute revenue_per_send & conversion_rate_pct
    for r in rows:
        aud = r["audience_size"]
        r["revenue_per_send"] = round(r["attributed_revenue"] / aud, 4) if aud else None
        r["conversion_rate_pct"] = round(r["redemptions"] / aud * 100, 4) if aud else None

    # Assign ranks
    from collections import defaultdict
    by_period = defaultdict(list)
    by_period_loc = defaultdict(list)
    for r in rows:
        by_period[r["period"]].append(r)
        by_period_loc[(r["period"], r["location_id"])].append(r)

    for period, period_rows in by_period.items():
        sorted_rows = sorted(period_rows, key=lambda r: -r["attributed_revenue"])
        for i, r in enumerate(sorted_rows, start=1):
            r["rank_in_period"] = i

    for key, loc_rows in by_period_loc.items():
        sorted_rows = sorted(loc_rows, key=lambda r: -r["attributed_revenue"])
        for i, r in enumerate(sorted_rows, start=1):
            r["rank_in_location_period"] = i

    # Sort by period DESC, attributed_revenue DESC
    rows.sort(key=lambda r: (r["period"], r["attributed_revenue"]), reverse=True)
    return rows


PROMO_ATTRIBUTION_MONTHLY = {
    "business_id":   42,
    "period_start":  "2025-01-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data":          _build_promo_attribution(),
}


# ─────────────────────────────────────────────────────────────────────────────
# Business 99 — minimal rows for tenant isolation testing
# ─────────────────────────────────────────────────────────────────────────────
# IDs intentionally don't overlap with biz 42's campaigns/promos.
# These should never appear when business_id=42 is requested.

CAMPAIGN_SUMMARY_99 = {
    "business_id":   99,
    "period_start":  "2026-03-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data": [
        {
            "campaign_id": 9001, "execution_date": "2026-03-20",
            "period": "2026-03-01", "campaign_name": "Biz99-Campaign-A",
            "campaign_status": "completed", "is_active": 1, "is_recurring": 0,
            "channel": "email", "channel_code": 1,
            "template_format_name": "Biz99-Template",
            "audience_size": 500, "promo_code_string": "BIZ99PROMO",
            "campaign_start": "2026-03-15", "campaign_expiration": "2026-04-30",
            "total_sent": 500, "delivered": 490, "failed": 10,
            "opened": 150, "clicked": 22,
            "delivery_rate_pct": 98.0, "open_rate_pct": 30.61,
            "click_rate_pct": 4.49, "ctr_engagement_pct": 14.67,
            "is_expired_but_active": 0,
            "rank_open_in_period": 1, "rank_click_in_period": 1,
            "rank_reach_in_period": 1,
        },
    ],
}

CHANNEL_MONTHLY_99 = {
    "business_id":   99,
    "period_start":  "2026-03-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data": [
        {
            "period": "2026-03-01",
            "emails_sent": 500, "sms_sent": 120,
            "prev_emails_sent": 450, "prev_sms_sent": 100,
            "emails_mom_pct": 11.11, "sms_mom_pct": 20.0,
            "email_campaigns_run": 1, "email_open_rate_pct": 30.61,
            "email_click_rate_pct": 4.49,
            "sms_campaigns_run": 1, "sms_open_rate_pct": None,
            "sms_click_rate_pct": 5.0,
            "email_unsubscribed_count": 8, "sms_unsubscribed_count": 3,
            "total_contacts": 580, "email_contactable": 565,
            "sms_contactable": 575,
            "email_net_unsub_delta": 1, "sms_net_unsub_delta": 0,
            "email_contactable_mom_pct": 0.18,
        },
    ],
}

PROMO_ATTRIBUTION_MONTHLY_99 = {
    "business_id":   99,
    "period_start":  "2026-03-01",
    "period_end":    "2026-03-31",
    "generated_at":  "2026-04-20T10:00:00Z",
    "data": [
        {
            "campaign_id": 9001, "period": "2026-03-01", "location_id": 10,
            "campaign_name": "Biz99-Campaign-A",
            "promo_code_string": "BIZ99PROMO", "audience_size": 500,
            "redemptions": 12, "attributed_revenue": 620.00,
            "total_discount_given": 120.00, "net_revenue_after_discount": 500.00,
            "revenue_per_send": 1.24, "conversion_rate_pct": 2.4,
            "rank_in_period": 1, "rank_in_location_period": 1,
        },
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture lookup — endpoint path → response
# ─────────────────────────────────────────────────────────────────────────────

FIXTURES: dict[str, dict] = {
    "/api/v1/leo/marketing/campaign-summary":           CAMPAIGN_SUMMARY,
    "/api/v1/leo/marketing/channel-monthly":            CHANNEL_MONTHLY,
    "/api/v1/leo/marketing/promo-attribution-monthly":  PROMO_ATTRIBUTION_MONTHLY,
}


# Exported for visibility
__all__ = [
    "CAMPAIGN_SUMMARY",
    "CAMPAIGN_SUMMARY_99",
    "CHANNEL_MONTHLY",
    "CHANNEL_MONTHLY_99",
    "PROMO_ATTRIBUTION_MONTHLY",
    "PROMO_ATTRIBUTION_MONTHLY_99",
    "FIXTURES",
]
