"""
Query Analyzer / Router
=======================
Determines the correct processing path for each incoming question.

Routes:
  - DIRECT  → General knowledge / advice, no business data needed
  - RAG     → Requires analysis of the tenant's own business data
  - AGENT   → (V2, post-MVP) Multi-step reasoning with tool use

Architecture (per spec):
  Step 1 — Rule-based check      (~1 ms,   no I/O)
  Step 2 — Classifier fallback   (~50–100 ms, LLM call)

The classifier is only invoked when rules are inconclusive, keeping
the happy-path latency as low as possible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route enum
# ---------------------------------------------------------------------------

class Route(str, Enum):
    DIRECT = "DIRECT"
    RAG    = "RAG"
    AGENT  = "AGENT"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AnalysisResult:
    route:            Route
    confidence:       float
    method:           str
    matched_keywords: list[str]      = field(default_factory=list)
    latency_ms:       float          = 0.0
    reasoning:        Optional[str]  = None


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------

REVENUE_KEYWORD_GROUP: dict[str, list[str]] = {
    "financial": [
        # Existing terms — kept as-is
        "revenue", "profit", "income", "earnings", "sales", "turnover",
        # Step 7 Fix 2e — removed "expense" (belongs in expenses group)
        # and "payment" (kept in expenses via "payment method"/"paid in cash")
        "margin", "cost", "invoice", "refund",
        "price", "pricing", "discount", "cashflow", "cash flow",

        # Revenue-specific — covers all 20 Step 1 questions
        "total revenue", "service revenue", "gross revenue",
        "average ticket", "avg ticket", "ticket value", "ticket size",
        "per visit", "per appointment",

        # Trends & growth (Q5, Q16, Q17)
        "trending", "trend", "growing", "growth", "shrinking",
        "going up", "going down", "revenue trend",
        "business growing", "business shrinking",

        # Period comparisons (Q4, Q7)
        "month over month", "mom", "year over year", "yoy",
        "change in revenue", "compare", "comparison",

        # Payment types (Q10)
        # Step 7 Fix 2e — removed "payment type", "payment method",
        # "payment breakdown" (they belong to expenses group and were
        # causing multi-domain pollution on Q15). "cash"/"card"/"credit card"
        # kept because Revenue Q10 asks about payment mix in revenue context.
        "cash", "card", "credit card",

        # Gift cards (Q11, LQ10)
        "gift card", "gift cards", "gift card redemption", "redeemed",

        # Promos & discounts (Q12, LQ9)
        "promo", "promo code", "promotion", "coupon", "promo cost",
        "discount cost",

        # Tips (Q18)
        "tips", "tip", "gratuity", "staff tips",

        # Tax (Q19)
        "tax", "taxes", "tax collected",

        # Failed / refunded (Q20, Q15)
        "refunded", "failed payment", "canceled", "cancelled",
        "no-show", "no show", "missed appointment", "lost revenue",

        # Root cause (Q13, Q14)
        "revenue dropped", "revenue fell", "revenue decrease",
        "revenue increase", "less busy", "fewer visits",
        "why did revenue", "why was revenue",

        # Staff revenue (Q8)
        "who made the most", "top staff", "staff revenue",
        "which employee", "which staff", "best performer",

        # Location revenue (Q9, LQ1–LQ10)
        "which location", "by location", "location revenue",
        "top location", "best location", "worst location",
        "location breakdown", "location comparison",

        # Advice (Q16, Q17)
        "increase revenue", "improve revenue", "boost revenue",
        "should i be worried", "is my business",
    ]
}

GIFTCARDS_KEYWORDS: list[str] = [
    # ── Core gift card vocabulary (overlaps with Revenue's "financial") ──
    "gift card", "gift cards", "giftcard", "giftcards", "gc",

    # ── Vocab variants (Q18-Q21 — Lesson 6) ──
    "prepaid", "prepaid card", "prepaid cards",
    "stored value", "stored value card", "stored-value",
    "gift voucher", "gift vouchers", "voucher card",
    "gift certificate", "gift certificates",

    # ── Outstanding / liability (Q2, Q3, Q6, Q19, Q22) ──
    "outstanding", "outstanding liability", "outstanding gift card",
    "outstanding balance", "liability", "gift card liability",
    "unused balance", "remaining balance", "unredeemed", "unredeemed balance",
    "owe", "owed", "money on cards", "money on gift cards",

    # ── Issuance / activation / sales (Q1, Q5) ──
    "issued", "gift cards issued", "activated", "gift card activated",
    "sold gift cards", "gift card sales", "new gift cards",
    "gift cards sold",

    # ── Redemption (Q4, Q5, Q7) — shared with Revenue ──
    "redemption", "redemptions", "redeemed", "gift card redemption",
    "gift card redemptions", "drained", "fully used", "fully redeemed",
    "gift card revenue", "gift card spend",

    # ── Trends & comparisons (Q4-Q7) ──
    "gift card trend", "redemption trend", "vs last year",
    "year over year gift card", "month over month gift card",

    # ── Per-staff (Q8) ──
    "top staff gift card", "which staff redeems", "who redeems gift cards",
    "staff gift card", "staff redemptions",

    # ── Per-location (Q9, Q10, S3) — Lesson 5: include "branch" + "location" ──
    "by location gift card", "by branch gift card", "branch gift card",
    "location gift card", "where redeemed", "which branch redeems",
    "which location redeems", "top location gift card",
    "top branch gift card",

    # ── Denomination (Q12) ──
    "denomination", "denominations", "face value", "face values",
    "card amount", "card value", "card denomination",
    "$25 gift card", "$50 gift card", "$100 gift card",
    "common denomination", "popular denomination",

    # ── Why / root cause (Q13-Q15) ──
    "why gift card", "gift card down", "gift card up",
    "gift card spike", "gift card dropped",

    # ── Aging / dormant (Q14, Q15, Q26, Q28) ──
    "dormant", "dormant gift cards", "expired gift card",
    "expire", "expiration", "old gift card", "old gift cards",
    "untouched gift cards", "sitting gift cards", "sitting unused",
    "never used", "never redeemed", "never-redeemed",
    "aging gift cards", "gift card age", "how long gift card",
    "days to first redemption", "first redemption",

    # ── Advice (Q16, Q17) ──
    "should i give gift cards", "should i sell gift cards",
    "what to do with dormant gift cards",
    "promote gift cards", "gift card strategy",

    # ── Anomalies (Q24, Q25, Q31) ──
    "deactivated gift card", "deactivated gift cards",
    "drained but active", "drained-but-active", "anomaly gift card",
    "gift card anomaly", "refunded gift card",
    "refunded gift cards", "gift card refund",

    # ── Health / pattern (Q23, Q30) ──
    "redemption rate", "gift card redemption rate",
    "single visit", "single-visit", "single visit drained",
    "multi visit", "multi-visit", "drained in one visit",
    "drained in single visit", "how cards are used",
    "gift card usage pattern",

    # ── Uplift / customer spend (Q27) ──
    "uplift", "gift card uplift", "spend on top of gift card",
    "out of pocket gift card", "extra spending gift card",

    # ── Time modifiers shared with revenue ──
    "this month gift card", "last month gift card",
    "this quarter gift card",
]

PROMOS_KEYWORDS: list[str] = [
    # ── Core domain vocabulary ────────────────────────────────────────────
    "promo", "promos", "promotion", "promotions",
    "promo code", "promo codes",
    "coupon", "coupons", "coupon code", "coupon codes",
    "discount", "discounts", "discounting",
    "redemption", "redemptions", "redeemed", "redeem",
    "offer", "offers",
    "deal", "deals",
    "savings",

    # ── Specific code references (from the real biz-42 catalog) ──────────
    "dm8880", "pm8880", "awan", "pofl99", "dm881", "DM8880", "PM8880", "Awan", "POFL99", "DM881",

    # ── Count / volume questions (Q1, Q9, Q10, Q13) ──────────────────────
    "how many promos", "how many redemptions", "how many coupons",
    "promo count", "redemption count",
    "most redeemed", "most popular promo", "most used promo",
    "most used code", "top promo", "top promo code",
    "top coupon", "best promo",
    "least redeemed", "least used", "rarely used",

    # ── Amount / dollar questions (Q2, Q4, Q11, Q15, Q24, Q25) ───────────
    "total discount", "total discounts", "total discount given",
    "discount given", "total savings",
    "biggest discount", "largest discount", "biggest single discount",
    "average discount", "avg discount", "average discount per",
    "discount per redemption", "average coupon savings",
    "how much did we discount", "how much in discounts",
    "discount amount", "discount total",
    "promo cost", "cost of promos", "what did promos cost",

    # ── Distinct codes & catalog (Q3, Q22, Q23) ──────────────────────────
    "distinct codes", "distinct promos", "different codes",
    "different promos", "how many codes", "how many promo codes",
    "active promos", "active promo codes", "inactive promo",
    "expired promo", "expired code", "expired promo code",
    "expiring soon",
    "active but expired", "active-but-expired",
    "dormant promo", "dormant code", "dormant promo code",
    "unused promo", "unused code", "stale promo", "stale code",
    "which codes should I retire",
    "promos i am not using", "codes i am not using",
    "retire code", "retire promo", "deactivate promo",

    # ── Trend / temporal questions (Q5-Q8, Q12, Q26) ─────────────────────
    "promo trend", "promo activity",
    "redemption trend", "coupon usage trend",
    "promos last month", "promos this month",
    "promos this year", "promo usage ytd",
    "promo usage over time", "promo volume over time",
    "best month for promos", "worst month for promos",
    "peak promo month",
    "promo visit percentage", "promo visit pct",
    "% of visits with promo", "percent of visits using promo",
    "visits using promos", "visits with a promo",
    "percent of customers using promo", "customers using promos",

    # ── Root cause questions (Q14, Q15) ──────────────────────────────────
    "why did promo", "why did redemptions",
    "why did discounts spike", "why did discount spike",
    "why more promos", "why fewer promos",
    "promo spike", "redemption spike", "discount spike",
    "promo drop", "redemption drop",
    "which code drove", "which promo drove",
    "which promo jumped", "which promo fell",

    # ── Location / branch questions (Q18-Q21) ────────────────────────────
    "promos by location", "promos per location",
    "promos by branch", "promos per branch",
    "promo redemption by location", "promo redemption per location",
    "discount by location", "discount per location",
    "discount by branch", "discount per branch",
    "which location redeems", "which branch redeems",
    "which branch gives the most discount",
    "which location gives the most discount",
    "main street promos", "westside promos",
    "main st promos", "westside coupons",
    "branch promo", "location promo",

    # ── Lifecycle / code-level questions (Q22, Q23) ──────────────────────
    "promo expired",
    "promo still active",
    "code lifecycle",

    # ── Advice questions (Q16, Q17) ──────────────────────────────────────
    "should I retire promo", "should we retire promo",
    "which promos to keep", "which promos to stop",
    "promo strategy", "coupon strategy",
    "promo recommendation", "promo advice",

    # ── Data integrity / edge cases (Q24-Q26) ────────────────────────────
    "unknown promo code", "orphan promo",
    "promo without a code",
    "promos on refunded visits", "promos on cancelled visits",
    "avg discount per redemption",
]

RAG_KEYWORD_GROUPS: dict[str, list[str]] = {
    "financial": REVENUE_KEYWORD_GROUP["financial"],
    "appointments": [
        # Core booking vocabulary (Q1–Q4)
        "appointment", "appointments", "booking", "bookings",
        "booked", "book", "scheduled", "schedule",
        "how many appointments", "appointment count", "appointment volume",

        # Status terms (Q2, Q24–Q26)
        "cancellation", "cancellations", "cancellation rate", "cancel rate",
        "cancelled", "canceled", "no-show", "no show", "no shows",
        "no-shows", "noshows", "did not show", "missed appointment",
        "completed appointment", "completed appointments",
        "completion rate", "incomplete",

        # Trend questions (Q5–Q8, Q9, Q10)
        "trending", "trend", "going up", "going down",
        "more appointments", "fewer appointments", "less bookings",
        "more bookings", "appointment trend", "booking trend",
        "declining appointments", "growing appointments",
        "appointment growth", "appointment decline",

        # Time slot distribution (Q11, Q12)
        "morning appointments", "afternoon appointments", "evening appointments",
        "morning slot", "afternoon slot", "evening slot",
        "time slot", "time slots", "peak time", "busiest time",
        "busy period", "quiet period", "slow period",
        "weekend appointments", "weekday appointments",
        "weekends", "weekend bookings", "weekend vs weekday",

        # Staff appointment questions (Q13–Q18)
        "which staff", "which employee", "who had the most appointments",
        "who completed the most", "staff appointments", "employee appointments",
        "most appointments", "fewest appointments", "least appointments",
        "staff no-show", "employee no-show", "staff cancellation",
        "staff performance", "staff decline", "declining staff",
        "staff booking", "staff booked", "who is handling",
        "appointments per staff", "appointments per employee",

        # Service questions (Q19–Q23, Q26)
        "which service", "most booked service", "popular service",
        "most popular service", "service bookings", "service appointments",
        "service duration", "how long does", "average duration",
        "duration per service", "minutes per service", "appointment length",
        "how long is",
        "service cancellation", "service cancel rate",
        "repeat clients", "repeat bookings", "returning clients",
        "clients coming back", "most requested service",
        "service frequency", "seasonal service", "service trend",

        # Location breakdown (Q25, Q27–Q29)
        # Step 7 Fix 2e — removed bare "which branch" (matched expense
        # questions like Q16 "which branch costs more to run"). Kept
        # "branch appointments"/"location appointments" for appointment-
        # specific branch questions, and bare "which location" is still
        # useful since it doesn't overlap with expense vocabulary.
        "which location", "location appointments",
        "branch appointments", "location bookings", "branch bookings",
        "busiest branch", "busiest location", "location cancellation",
        "location cancel rate", "location comparison",
        "compare locations", "locations this month",
        "location volume", "branch volume",

        # Walk-in vs app (Q11 related)
        "walk-in", "walk in", "walkin", "walk-ins", "app booking",
        "app bookings", "online booking", "online bookings",
        "how did they book", "booking source", "booking channel",

        # Rescheduling / operational
        "rescheduled", "rescheduling", "rebook", "rebooking",
    ],
    "clients": [
        # ── Core client vocabulary ─────────────────────────────────────────
        "client", "clients", "customer", "customers",
        "patron", "patrons", "guest", "guests",
        "client base", "customer base", "client roster",
        "on file", "on our books",

        # ── Counts & status (Q1, Q2, Q3, Q17) ──────────────────────────────
        "total clients", "total customers", "total customer count",
        "clients ever", "customers ever", "all time clients", "all-time clients",
        "how many clients", "how many customers",
        "active clients", "active customers",
        "new client", "new clients", "new customer", "new customers",
        "returning clients", "returning customers",
        "first-time", "first time", "first-time client",

        # ── Acquisition (Q2, Q4, Q10, Q20) ─────────────────────────────────
        "acquisition", "client acquisition", "customer acquisition",
        "acquired", "new client acquisition",
        "new client drop", "fewer new clients",
        "new clients this month", "new clients last month",

        # ── LTV / top spenders (Q7, Q19) ───────────────────────────────────
        "lifetime value", "lifetime spend", "lifetime revenue", "LTV", "ltv",
        "biggest spenders", "top spenders", "best customers", "best clients",
        "big spenders", "high value clients", "high-value clients",
        "top 10 clients", "top clients", "VIP", "VIPs", "whales",
        "regulars", "loyal clients",
        "top 10 percent", "top 10%", "top tier",
        "revenue concentration",

        # ── Frequency & visits (Q8, Q23) ───────────────────────────────────
        "most frequent", "most frequent clients", "most visits",
        "frequent visitors", "top visitors",
        "unique clients", "unique customers", "unique visitors",
        "unique people", "different customers", "different clients",
        "distinct clients", "distinct customers",
        "how many people", "how many different",
        "walk-in", "walk in", "walkin", "walk-ins",

        # ── Points / rewards (Q9) ──────────────────────────────────────────
        "loyalty points", "points", "rewards", "rewards points",
        "points balance", "reward balance", "loyalty balance",
        "top points holders", "most points",

        # ── Churn, retention, at-risk (Q5, Q11, Q12) ───────────────────────
        "retention", "retention rate", "cohort retention", "return rate",
        "churn", "churn rate", "churning",
        "at risk", "at-risk", "at-risk clients", "risk of churning",
        "haven't seen", "haven't been back", "hasn't been back",
        "gone quiet", "lost interest",
        "dormant client", "dormant customer", "inactive client",
        "lost client", "lost customer", "losing clients",
        "about to churn",

        # ── Reactivation / win-back (Q6, Q14) ──────────────────────────────
        "reactivated", "reactivation", "reactivate",
        "came back", "came back after", "returned after",
        "win back", "win-back", "winback", "won back",
        "reach out", "email outreach", "outreach campaign",

        # ── Reachability (Q18) ─────────────────────────────────────────────
        "reachable", "contactable", "can we email", "can we text",
        "unsubscribed", "opted out", "opt-out",
        "email list", "SMS list", "email-able",

        # ── Mix / composition (Q16, Q21, Q22) ──────────────────────────────
        "new vs returning", "new versus returning", "new and returning",
        "age distribution", "age breakdown", "age bracket",
        "demographics", "client demographics", "customer demographics",
        "age of new clients", "new client age",
        "member", "members", "membership status", "active members",
        "how many members", "member overlap",

        # ── Advice (Q15) ───────────────────────────────────────────────────
        "improve retention", "retain more clients", "keep clients",
        "retention advice", "stop the churn",

        # ── Per-location (Q20) ─────────────────────────────────────────────
        "which branch new", "which location new", "new clients by location",
        "new clients by branch", "location acquisition",

        # ── MoM language (Q4, Q10) ─────────────────────────────────────────
        "client drop", "customer drop", "new client change",
        "acquisition dropped", "acquisition fell",
    ],
    "expenses": [
        # ── Core domain words ────────────────────────────────────────────────
        "expense", "expenses",
        "cost", "costs", "costing",
        "spending", "spend", "spent",
        "overhead", "overheads",
        "outflow", "outflows", "outgoing",
        "bill", "bills", "billing",
        "payable", "payables",
        "expenditure", "expenditures",
        "operating cost", "operating costs",
        "operating expense", "operating expenses",
        "opex",
        "cogs",
        "money out", "money going out",
        "where does my money go",

        # ── Category-level vocabulary ───────────────────────────────────────
        "rent", "rental",
        "utilities", "utility",
        "electricity", "electric bill", "power bill",
        "water bill", "internet bill",
        "marketing spend", "ad spend", "advertising spend",
        "supplies", "product supplies", "office supplies",
        "insurance", "insurance premium",
        "equipment", "equipment cost", "equipment purchase",
        "payroll", "payroll cost", "salary cost", "wages",
        "commission", "commissions",
        "software", "subscriptions", "software cost",
        "office cost", "admin cost", "administrative",
        "maintenance",
        "repair", "repairs",
        "training", "training cost",
        "travel", "travel expense",

        # ── Anomaly / trend framing (Q22, Q23, Q24, Q25) ────────────────────
        "spike", "spiked", "spiking",
        "spent more", "spent less",
        "higher than usual", "lower than usual",
        "unusually high", "unusually low",
        "more than usual", "less than usual",
        "elevated",
        "anomaly", "anomalous",
        "abnormal",
        "unexpected expense", "unexpected cost",
        "increase in spending", "decrease in spending",
        "expense growth", "cost reduction",

        # ── Dormancy / inactivity (Q28) ─────────────────────────────────────
        "dormant", "dormant category",
        "stopped spending", "no longer spending",
        "haven't spent", "haven't paid",
        "silent category", "inactive category",
        "stopped logging",
        "gone quiet",

        # ── Comparison / ranking ────────────────────────────────────────────
        "biggest expense", "biggest cost",
        "largest expense", "largest cost",
        "most expensive",
        "top expense", "top expenses",
        "smallest expense",
        "expense breakdown", "cost breakdown",
        "spending breakdown",
        "by category", "by location", "by branch",
        "expense ranking", "cost ranking",

        # ── Time-bound expense phrases ──────────────────────────────────────
        "this month's expenses", "this month spending",
        "last month's expenses", "last month spending",
        "monthly expenses", "monthly costs", "monthly bills",
        "year-to-date expenses", "ytd expenses", "ytd spending",
        "quarterly expenses", "quarterly spending",
        "qoq expenses",
        "this quarter spending", "last quarter spending",

        # ── Payment method (Q14, Q15) ───────────────────────────────────────
        "paid in cash", "paid by check", "paid by card",
        "paid with cash", "paid with check", "paid with card",
        "cash spending", "check spending", "card spending",
        "payment method", "payment type",
        "how do I pay",
        "cash vs card", "cash vs check",

        # ── Staff attribution (Q26, NOT Q27) ────────────────────────────────
        "who logs", "who logged", "who logs expenses",
        "who entered", "who recorded",
        "expense logger", "expense entry",
        "data entry expenses",

        # ── Causal / advice (Q23, Q25, Q30, Q31, Q32) ───────────────────────
        "why did expenses",
        "why did costs",
        "what drove",
        "explain my expenses",
        "explain my costs",
        "where can I cut costs",
        "where to cut spending",
        "reduce expenses", "reduce costs", "cut expenses",
        "save money",
        "optimize spending", "optimize costs",
        "expense advice",

        # ── Data-quality / honesty (Q29) ────────────────────────────────────
        "duplicate expense", "duplicate expenses",
        "duplicate entry", "duplicate entries",
        "miscategorized",
        "wrong category",
        "expense mistake",
        "double-counted",

        # ── Location-scoped (Q16-Q19) ───────────────────────────────────────
        "expenses by location", "expenses per location",
        "expenses per branch", "expenses by branch",
        "main st expenses", "westside expenses",
        "branch expenses", "branch costs",
        "which location spends",
        "which branch spends",
    ],
    "giftcards": GIFTCARDS_KEYWORDS,
    "staff": [
        # Core staff vocabulary
        "staff", "employee", "employees", "team", "stylist", "therapist",
        "technician", "performance", "utilisation", "utilization",
        "rating", "reviews", "rating score",

        # Vocabulary variants (Q25-Q32 test cases)
        "worker", "workers",
        "mvp",
        "slacking", "slacked",
        "underperforming", "underperformer",

        # Attendance / hours (Q33)
        "hours", "clocked", "hours worked", "hours clocked",
        "clock in", "clock out", "attendance hours", "working hours",

        # Commission (Q34, Q36, Q37 — improves domain targeting to staff+revenue)
        "commission", "commission earned", "commission rate",

        # Active / inactive status (Q5, Q21)
        "active staff", "inactive staff", "deactivated",

        # Hire / tenure (Q22)
        "joined", "hire date", "new hire",

        # Cross-domain root cause (Q38)
        "staffing", "staffing issue",
    ],
    "services": [
        # Core vocabulary
        "service", "services", "treatment", "treatments", "menu",
        "service menu", "service list", "service catalog",
        "popularity", "upsell", "add-on", "add on", "package",

        # Revenue & pricing (Q6–Q10)
        "service revenue", "service price", "list price", "charged price",
        "avg price", "average price", "discounted", "discounting",
        "most discounted", "price difference",

        # Margin & commission (Q11–Q13)
        "profitable service", "most profitable", "service margin",
        "margin by service", "margin by category", "commission cost",
        "commission percentage", "after commission",

        # Trends (Q14–Q17)
        "service trend", "service trending", "service growing",
        "service declining", "biggest jump", "fastest growing service",

        # Repeat clients (Q18)
        "repeat clients by service", "most repeat",

        # Co-occurrence / combos (Q19)
        "booked together", "performed together", "combo",
        "paired with", "co-occurrence", "commonly booked",

        # First service (Q20)
        "first service", "new client first", "what do new clients book",

        # Staff × service (Q21–Q23)
        "who performs", "specializes in", "specialise",
        "only one staff", "single staff",

        # Location × service (Q24–Q26)
        "popular at branch", "popular at location",
        "service at location", "service by branch",
        "not offered", "location gap",

        # Duration (Q27–Q28)
        "runs longer", "over schedule", "actual duration",
        "scheduled vs actual", "service duration",
        "how long does", "takes longer",

        # Catalog health (Q29–Q30)
        "dormant", "dormant service", "hasn't sold",
        "no sales", "inactive service", "discontinued",
        "new service", "recently added", "new this year",
        "added this year", "add this year", "introduced this year",
        "new services", "added to the menu", "added to menu",
        "service performance",

        # Category (Q10, Q12)
        "service category", "by category", "category breakdown",
        "skincare", "massage", "hair", "nails",
    ],
    "time_comparisons": [
        "this month", "last month", "this week", "last week",
        "this quarter", "last quarter", "year to date", "ytd",
        "compared to", "vs last", "versus", "trend", "trends",
        "month on month", "week on week", "quarter on quarter",
        "year over year", "yoy", "period",
    ],
    "marketing": [
        # ── Core campaign vocabulary ────────────────────────────────────────
        "campaign", "campaigns", "marketing", "blast", "blasts",
        "send", "sends", "sent", "send volume",
        "promotion", "promotions", "promo", "promo code",

        # ── Channel — Email ─────────────────────────────────────────────────
        "email", "emails", "email campaign", "email campaigns",
        "email marketing", "email blast", "newsletter",
        "email volume", "email send", "emails sent",
        "email open", "email click", "email performance",

        # ── Channel — SMS / Mobile ──────────────────────────────────────────
        "sms", "sms campaign", "sms campaigns", "text", "texts",
        "text message", "text messages", "text blast",
        "sms volume", "mobile",
        "sms vs email", "email vs sms",

        # ── Performance KPIs (Q5–Q13, Q19, Q20, Q33) ────────────────────────
        "open rate", "open rates", "opens", "opened",
        "click rate", "click rates", "clicks", "clicked",
        "click through", "click-through", "ctr",
        "delivery rate", "delivery success",
        "delivered", "deliverability",
        "failed", "fail rate", "failure rate",
        "bounced", "bounce rate",
        "reach", "reached", "reach rate",
        "audience", "audience size",
        "engagement", "engagement rate",

        # ── Unsubscribes / list health (Q27, Q28, Q34) ──────────────────────
        "unsubscribe", "unsubscribes", "unsubscribed", "unsub",
        "opt out", "opt-out", "opted out",
        "contactable", "email-contactable", "email list",
        "sms list", "marketing list", "contact list",
        "list health", "list growth", "list shrinking",
        "list fatigue", "over sending", "over-sending",
        "too many emails", "sending too much",
        "to my customers", "to customers", "sending to customers",
        "email my customers", "text my customers",

        # ── ROI / Attribution (Q15–Q18, Q30) ────────────────────────────────
        "revenue from campaign", "campaign revenue",
        "attributed revenue", "promo revenue",
        "redeemed", "redemption", "redemptions",
        "pay for itself", "paid off",
        "revenue per send", "revenue per campaign",
        "campaign cost", "promo cost",
        "conversion", "conversions", "conversion rate",
        "impressions", "ad spend", "roi",

        # ── Rankings (Q10, Q11, Q12, Q13, Q14) ──────────────────────────────
        "best campaign", "best-performing campaign",
        "best performing campaign", "top campaign", "top-performing",
        "worst campaign", "worst-performing",
        "highest open rate", "highest click rate",
        "lowest open rate", "most opens", "most clicks",
        "most redeemed", "most redemptions", "most-redeemed",

        # ── Recurring / templates (Q14, Q25, Q26) ───────────────────────────
        "recurring campaign", "recurring campaigns",
        "welcome series", "drip campaign",
        "automated campaign", "triggered campaign",
        "still performing", "last run",
        "template", "templates", "template format",
        "campaign format", "email format",

        # ── Trend language (Q19, Q20, Q21, Q22) ─────────────────────────────
        "open rates going up", "open rates going down",
        "click rates improving", "click rate improving",
        "open rate trend", "click rate trend",
        "engagement trend", "send volume trend",

        # ── Branch / location (Q29, Q30) ────────────────────────────────────
        "campaign by location", "location campaigns",
        "branch campaigns", "campaigns per branch",
        "promo redemptions by location", "promo by branch",

        # ── Day-of-week (Q33) ───────────────────────────────────────────────
        "day of the week", "best day to send", "best send day",
        "weekday performance",

        # ── Advice (Q31, Q32) ───────────────────────────────────────────────
        "improve open rates", "improve click rates",
        "boost open rates", "increase engagement",
        "underperform", "underperformed",
        "why did my campaign", "what went wrong with campaign",
    ],
    "promos": PROMOS_KEYWORDS,
    "analytics": [
        "report", "reports", "analytics", "analysis", "breakdown",
        "summary", "overview", "dashboard", "metric", "metrics", "kpi",
        "forecast", "projection", "predict", "prediction",
        "decrease", "increase", "decline", "growth", "drop", "spike",
        "why did", "what caused", "what happened",
    ],
}

_RAG_KEYWORDS: frozenset[str] = frozenset(
    kw for group in RAG_KEYWORD_GROUPS.values() for kw in group
)

DIRECT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bhow (can|do|should|could) (salons?|spas?|barbershops?|businesses?)\b", re.I),
    re.compile(r"\bwhat (are|is) (the )?(best|good|common|typical|average)\b", re.I),
    re.compile(r"\btips? (for|on|to)\b", re.I),
    re.compile(r"\badvice (for|on|about)\b", re.I),
    re.compile(r"\bindustry (standard|average|benchmark|best practice)\b", re.I),
    re.compile(r"\bin general\b", re.I),
    re.compile(r"\bexplain (what|how|why)\b", re.I),
    re.compile(r"\bwhat does .+ mean\b", re.I),
    re.compile(r"\bdefine\b", re.I),
]

MY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bmy (revenue|profit|clients?|staff|appointments?|bookings?|services?|business|salon|spa|shop|team|cancellations?|performance|data|report|metrics?|kpi)\b", re.I),
    re.compile(r"\b(our|we|i have|i've|i'm)\b", re.I),
    # Fuzzy possessive + location noun — catches "my downtown location", "my Main St branch"
    re.compile(r"\bmy\b.{0,20}\b(location|locations|branch|branches|site|sites)\b", re.I),
]

# NOTE: CLASSIFIER_SYSTEM_PROMPT removed — the prompt now lives in
# app/prompts/classifier/anthropic.py and app/prompts/classifier/openai.py.
# The gateway selects the correct one automatically via call_with_data().


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class QueryAnalyzer:
    """
    Classifies an incoming question and returns the correct Route.

    Usage
    -----
        analyzer = QueryAnalyzer()                        # rules only
        analyzer = QueryAnalyzer(gateway=my_gateway)      # + classifier fallback

    Parameters
    ----------
    gateway:
        Optional LLMGateway instance. When provided, ambiguous questions are
        sent to the classifier via gateway.call_with_data(UseCase.CLASSIFIER).
        If None, ambiguous questions fall back to RAG (safe default).

    confidence_threshold:
        Rule confidence must exceed this to skip the classifier. Default 0.75.
    """

    def __init__(
        self,
        gateway=None,
        confidence_threshold: float = 0.75,
    ) -> None:
        self._gateway = gateway
        self._confidence_threshold = confidence_threshold

    @property
    def confidence_threshold(self) -> float:
        """Minimum rule confidence before skipping the LLM classifier."""
        return self._confidence_threshold

    def preview_rule_routing(self, question: str) -> AnalysisResult:
        """
        Run only Step 1 (rule-based routing). Does not call the LLM classifier.

        Use this to debug why ``analyze()`` might choose RAG vs DIRECT before
        any gateway fallback or classifier call.
        """
        question = question.strip()
        if not question:
            return AnalysisResult(
                route=Route.DIRECT,
                confidence=1.0,
                method="rules",
                reasoning="Empty question.",
                latency_ms=0.0,
            )
        return self._rule_based_check(question)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def analyze(self, question: str, business_id: str = "") -> AnalysisResult:
        """
        Analyze a question and return the routing decision.

        Parameters
        ----------
        question:    The raw user question string.
        business_id: Tenant identifier — passed to the gateway for quota/logging.
        """
        t0 = time.perf_counter()
        question = question.strip()

        if not question:
            return AnalysisResult(
                route=Route.DIRECT,
                confidence=1.0,
                method="rules",
                reasoning="Empty question.",
                latency_ms=0.0,
            )

        # Step 1 — Rule-based check
        rule_result = self._rule_based_check(question)
        rule_result.latency_ms = (time.perf_counter() - t0) * 1000

        logger.debug(
            "query_analyzer.rules business_id=%s route=%s confidence=%.2f "
            "keywords=%s latency_ms=%.1f",
            business_id,
            rule_result.route,
            rule_result.confidence,
            rule_result.matched_keywords,
            rule_result.latency_ms,
        )

        if rule_result.confidence >= self._confidence_threshold:
            return rule_result

        # Step 2 — Classifier fallback
        if self._gateway is None:
            rule_result.route = Route.RAG
            rule_result.method = "rules_fallback"
            rule_result.reasoning = (
                "Rules inconclusive; no classifier configured — defaulting to RAG."
            )
            return rule_result

        classifier_result = await self._classifier_check(question, business_id)
        classifier_result.latency_ms = (time.perf_counter() - t0) * 1000

        logger.info(
            "query_analyzer.classifier business_id=%s route=%s confidence=%.2f "
            "latency_ms=%.1f reasoning=%r",
            business_id,
            classifier_result.route,
            classifier_result.confidence,
            classifier_result.latency_ms,
            classifier_result.reasoning,
        )

        return classifier_result

    # ------------------------------------------------------------------
    # Step 1 — Rule-based check
    # ------------------------------------------------------------------

    def _rule_based_check(self, question: str) -> AnalysisResult:
        q_lower = question.lower()

        # Definitional "explain what … means" — not a tenant metrics lookup.
        if re.search(r"\bexplain what .+ means?\b", q_lower):
            return AnalysisResult(
                route=Route.DIRECT,
                confidence=0.90,
                method="rules",
                reasoning="Definitional question — not a data lookup.",
            )

        # 1a-pre. Domain data override — runs BEFORE general-advice patterns.
        # "What is the average X" matches the DIRECT pattern BUT if X is a
        # business metric the question is about the tenant's own data.
        _DATA_METRIC_OVERRIDES = [
            "service duration", "appointment duration", "session duration",
            "average duration", "avg duration",
            "cancel rate", "cancellation rate", "no-show rate", "no show rate",
            "completion rate", "booking frequency", "appointment frequency",
            "per service type", "per staff", "per employee", "per location",
            "by service", "by staff", "by location", "by branch",
            # Staff multi-person queries — force single-domain staff search (top_k=12)
            # without these, "all staff members" hits both staff+financial groups
            # → multi-domain search → top_k_per_domain=3 → thin context → no answer
            "all staff members", "each staff member", "per staff member",
            "staff member's revenue", "staff working at",

            # ── Promos domain (Sprint 8) ──────────────────────────────────────
            "discount per redemption", "avg discount per redemption",
            "average discount per redemption",
            "total discount given", "biggest single discount",
            "most redeemed promo", "most redeemed code", "most redeemed coupon",
            "least redeemed promo", "least redeemed code",
            "promo visit percentage", "promo visit pct",
            "percent of visits using promo", "% of visits with promo",
            "redemption count", "redemption rate",
            "per promo code", "per coupon code", "by promo code",
            "by coupon", "by promo", "per promo", "per coupon",
            "distinct promo codes", "distinct coupons", "distinct promos",

            # ── Gift cards domain ─────────────────────────────────────────────
            "outstanding gift card balance", "outstanding gift card liability",
            "gift card liability",
            "gift cards i sold", "gift cards still active",
            "gift card redemption rate",
            "gift card distribution", "gift card denomination distribution",
            "dormant gift cards", "never-redeemed gift cards",
            "gift card uplift",
            "drained but active gift cards",
            "deactivated gift cards count",
            "refunded gift card redemptions",
        ]
        if any(phrase in q_lower for phrase in _DATA_METRIC_OVERRIDES):
            matched = [p for p in _DATA_METRIC_OVERRIDES if p in q_lower]
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.88,
                method="rules",
                matched_keywords=matched,
                reasoning="Domain metric override — business data question before general-advice check.",
            )

        # 1a. Strong DIRECT signals
        for pattern in DIRECT_PATTERNS:
            if pattern.search(question):
                if not any(p.search(question) for p in MY_PATTERNS):
                    return AnalysisResult(
                        route=Route.DIRECT,
                        confidence=0.90,
                        method="rules",
                        reasoning=f"Matched general-advice pattern: {pattern.pattern}",
                    )

        # 1b. Possessive / first-person → almost certainly RAG
        # But also run domain keyword matching so _resolve_domains can filter
        my_matches = [p.pattern for p in MY_PATTERNS if p.search(question)]
        if my_matches:
            # Also collect domain keywords so retriever can narrow the search
            domain_kws = [
                kw for kw in _RAG_KEYWORDS
                if re.search(r"\b" + re.escape(kw) + r"\b", q_lower)
            ]
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.95,
                method="rules",
                matched_keywords=my_matches + domain_kws,
                reasoning="Possessive/first-person pronoun detected.",
            )

        # 1c. Domain keyword matching
        matched = [
            kw for kw in _RAG_KEYWORDS
            if re.search(r"\b" + re.escape(kw) + r"\b", q_lower)
        ]

        if len(matched) >= 2:
            confidence = min(0.95, 0.75 + len(matched) * 0.04)
            return AnalysisResult(
                route=Route.RAG,
                confidence=confidence,
                method="rules",
                matched_keywords=matched,
            )

        if len(matched) == 1:
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.60,
                method="rules",
                matched_keywords=matched,
                reasoning="Single domain keyword matched — low confidence.",
            )

        # 1d. No signals
        return AnalysisResult(
            route=Route.DIRECT,
            confidence=0.55,
            method="rules",
            reasoning="No domain keywords or patterns matched",
        )

    # ------------------------------------------------------------------
    # Step 2 — LLM classifier fallback
    # ------------------------------------------------------------------

    async def _classifier_check(
        self, question: str, business_id: str
    ) -> AnalysisResult:
        """
        Calls the classifier via the gateway's prompt layer.

        The gateway resolves the correct provider-specific prompt
        (XML tags for Anthropic, plain text for OpenAI) automatically.
        """
        from app.prompts.types import ClassifierData
        from app.services.llm.types import UseCase

        try:
            data = ClassifierData(question=question)
            response = await self._gateway.call_with_data(
                UseCase.CLASSIFIER,
                data,
                business_id or "unknown",
            )

            payload = json.loads(response.content)
            route_str = str(payload.get("route", "RAG")).upper()
            route = Route.RAG if route_str == "RAG" else Route.DIRECT

            return AnalysisResult(
                route=route,
                confidence=float(payload.get("confidence", 0.80)),
                method="classifier",
                reasoning=payload.get("reasoning", ""),
            )

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "query_analyzer.classifier_error error=%r — defaulting to RAG", exc
            )
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.70,
                method="classifier_error",
                reasoning=f"Classifier failed ({exc}); defaulting to RAG.",
            )


# ---------------------------------------------------------------------------
# Convenience sync wrapper (for scripts / tests)
# ---------------------------------------------------------------------------

def analyze_sync(question: str, **kwargs) -> AnalysisResult:
    """Blocking wrapper around QueryAnalyzer.analyze() for non-async contexts."""
    analyzer = QueryAnalyzer(**kwargs)
    return asyncio.run(analyzer.analyze(question))


# ---------------------------------------------------------------------------
# FastAPI dependency factory
# ---------------------------------------------------------------------------

def get_query_analyzer(gateway=None) -> QueryAnalyzer:
    """
    FastAPI dependency. Wire into app startup or use Depends().

    Example
    -------
        from app.services.query_analyzer import get_query_analyzer, QueryAnalyzer
        from app.services.llm.llm_gateway import LLMGateway
        from fastapi import Depends

        gateway = LLMGateway.from_env()

        async def chat(
            payload: ChatRequest,
            analyzer: QueryAnalyzer = Depends(lambda: get_query_analyzer(gateway)),
        ):
            result = await analyzer.analyze(payload.question, payload.business_id)
            if result.route == Route.RAG:
                ...
    """
    return QueryAnalyzer(gateway=gateway)


# ---------------------------------------------------------------------------
# Quick smoke-test  (python query_analyzer.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_CASES: list[tuple[str, Route]] = [
        (
            "What is the average service duration for each service type?",
            Route.RAG,
        ),
        ("Why did my revenue decrease this month?",               Route.RAG),
        ("Which staff member generates the most repeat clients?",  Route.RAG),
        ("What services should I add to increase profitability?",  Route.RAG),
        ("How does my cancellation rate compare to last quarter?", Route.RAG),
        ("Show me a summary of last month's appointments.",        Route.RAG),
        ("Who are my top 5 clients by spend?",                    Route.RAG),
        ("How can salons improve customer retention?",             Route.DIRECT),
        ("What are the best practices for upselling?",             Route.DIRECT),
        ("Explain what a cancellation rate means.",                Route.DIRECT),
        ("What is the industry average for no-shows?",             Route.DIRECT),
        ("Give me tips on staff scheduling.",                      Route.DIRECT),
    ]

    analyzer = QueryAnalyzer()  # rules only for smoke test

    async def run():
        passed = failed = 0
        for question, expected in TEST_CASES:
            result = await analyzer.analyze(question, business_id="test")
            status = "✓" if result.route == expected else "✗"
            if result.route != expected:
                failed += 1
            else:
                passed += 1
            print(
                f"{status} [{result.route.value:<6}] conf={result.confidence:.2f} "
                f"method={result.method:<18} | {question}"
            )
            if result.reasoning:
                print(f"    → {result.reasoning}")
        print(f"\n{passed}/{passed+failed} passed")

    asyncio.run(run())