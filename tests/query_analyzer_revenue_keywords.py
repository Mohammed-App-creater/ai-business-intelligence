"""
app/services/query_analyzer.py  — REVENUE DOMAIN ADDITIONS
============================================================

Add the block below to RAG_KEYWORD_GROUPS inside your existing
query_analyzer.py. The retriever.py already reads from this dict
and maps "financial" → ["revenue"] domain in the vector store.

The keywords here are what a real business owner would type —
not technical terms. They must cover every question from Step 1.

INSTRUCTIONS:
    1. Open app/services/query_analyzer.py
    2. Find the RAG_KEYWORD_GROUPS dict
    3. Replace (or merge) the "financial" entry with the block below
    4. The retriever.py needs NO changes — it already maps
       "financial" → ["revenue"] in KEYWORD_GROUP_TO_DOMAINS
"""

# ── Paste this into RAG_KEYWORD_GROUPS in query_analyzer.py ─────────────────

REVENUE_KEYWORD_GROUP = {
    "financial": [
        # Core revenue terms
        "revenue", "income", "sales", "earnings", "money",
        "total revenue", "service revenue", "gross revenue",

        # Time-based revenue questions  (Q1, Q2, Q4–Q7)
        "last month", "this month", "this year", "ytd", "year to date",
        "this quarter", "last quarter", "same quarter last year",
        "best month", "worst month", "slowest month",

        # Ticket & visit value  (Q3, Q14)
        "average ticket", "avg ticket", "ticket value", "ticket size",
        "per visit", "per appointment",

        # Trend & growth  (Q5, Q16, Q17)
        "trending", "trend", "growing", "growth", "shrinking",
        "going up", "going down", "increasing", "decreasing",
        "revenue trend", "business growing", "business shrinking",

        # Period comparisons  (Q4, Q7)
        "compare", "comparison", "vs last month", "vs last year",
        "month over month", "mom", "year over year", "yoy",
        "change in revenue",

        # Payment types  (Q10)
        "cash", "card", "credit card", "payment type", "payment method",
        "how people pay", "payment breakdown",

        # Gift cards  (Q11, LQ10)
        "gift card", "gift cards", "gift card redemption", "gc",
        "redeemed",

        # Promos & discounts  (Q12, LQ9)
        "promo", "promo code", "discount", "promotion", "coupon",
        "promo cost", "discount cost", "how much did promos cost",

        # Tips  (Q18)
        "tips", "tip", "gratuity", "staff tips",

        # Tax  (Q19)
        "tax", "taxes", "tax collected", "sales tax",

        # Failed / refunded  (Q20, Q15)
        "refund", "refunded", "failed payment", "canceled", "cancelled",
        "no-show", "no show", "missed appointment",
        "lost revenue", "revenue lost",

        # Root cause / why questions  (Q13, Q14)
        "why did revenue drop", "why did revenue go down",
        "revenue dropped", "revenue fell", "revenue decrease",
        "why was revenue up", "revenue increase",
        "less busy", "busier", "more visits", "fewer visits",

        # Staff revenue ranking  (Q8)
        "who made the most", "top staff", "staff revenue",
        "which employee", "which staff", "best performer",
        "highest revenue staff",

        # Location revenue  (Q9, LQ1–LQ10)
        "which location", "by location", "location revenue",
        "top location", "best location", "worst location",
        "location breakdown", "location comparison",
        "location drop", "location performance",

        # Advice & recommendations  (Q16, Q17)
        "increase revenue", "improve revenue", "boost revenue",
        "what can i do", "should i be worried", "is my business",
        "recommendation", "advice",
    ]
}


# ── Full RAG_KEYWORD_GROUPS for reference ────────────────────────────────────
# This is what the complete dict looks like after merging.
# Copy the "financial" key into your existing dict.

RAG_KEYWORD_GROUPS: dict[str, list[str]] = {

    # ── Revenue domain (maps → ["revenue"] in retriever.py) ──────────────
    "financial": REVENUE_KEYWORD_GROUP["financial"],

    # ── Appointments domain (maps → ["appointments"]) ────────────────────
    # TODO: populate in Appointments domain sprint (Step 1)
    "appointments": [
        "appointment", "booking", "scheduled", "no-show",
        "cancellation", "cancel", "reschedule", "availability",
        "calendar", "upcoming", "today's appointments",
    ],

    # ── Clients domain (maps → ["clients"]) ──────────────────────────────
    "clients": [
        "client", "customer", "new client", "returning client",
        "retention", "repeat", "loyal", "churn", "lost client",
        "client count", "customer count",
    ],

    # ── Staff domain (maps → ["staff"]) ──────────────────────────────────
    "staff": [
        "staff", "employee", "team", "stylist", "therapist",
        "technician", "barber", "performance", "rating",
        "review", "hire", "fired",
    ],

    # ── Services domain (maps → ["services"]) ────────────────────────────
    "services": [
        "service", "treatment", "haircut", "massage", "facial",
        "manicure", "pedicure", "color", "popular service",
        "top service", "service revenue",
    ],

    # ── Marketing domain (maps → ["campaigns"]) ──────────────────────────
    "marketing": [
        "campaign", "email", "sms", "marketing", "promotion",
        "promo", "outreach", "open rate", "click rate",
        "message sent",
    ],

    # ── Broad / analytics (maps → None → search all domains) ─────────────
    "analytics": [
        "overview", "summary", "report", "dashboard", "analytics",
        "performance", "kpi", "metrics", "how is my business",
        "business health",
    ],

    # ── Time modifier (maps → None → search all domains) ─────────────────
    "time_comparisons": [
        "last week", "this week", "last month", "this month",
        "last year", "this year", "last quarter", "this quarter",
        "yesterday", "today", "recent", "latest", "compared to",
        "vs", "versus", "change", "trend",
    ],
}
