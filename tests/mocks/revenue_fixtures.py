"""
tests/mocks/revenue_fixtures.py

Realistic mock response data for all 6 revenue endpoints.
Based on a sample salon business (business_id=42) with 2 locations,
4 staff members, and 12 months of history.

Used by mock_analytics_server.py and test_revenue_etl.py.
"""

# ── /api/v1/leo/revenue/monthly-summary ──────────────────────────────────────
MONTHLY_SUMMARY = {
    "business_id": 42,
    "data": [
        {
            "period":           "2025-01",
            "visit_count":      178,
            "service_revenue":  11240.00,
            "total_tips":       1430.50,
            "total_tax":        898.00,
            "total_collected":  12670.50,
            "total_discounts":  220.00,
            "gc_redemptions":   180.00,
            "avg_ticket":       71.14,
            "mom_growth_pct":   None,       # first period — no previous
            "refund_count":     2,
            "cancel_count":     5,
        },
        {
            "period":           "2025-02",
            "visit_count":      163,
            "service_revenue":  10350.00,
            "total_tips":       1280.00,
            "total_tax":        828.00,
            "total_collected":  11630.00,
            "total_discounts":  190.00,
            "gc_redemptions":   140.00,
            "avg_ticket":       71.34,
            "mom_growth_pct":   -7.9,
            "refund_count":     3,
            "cancel_count":     7,
        },
        {
            "period":           "2025-03",
            "visit_count":      201,
            "service_revenue":  13480.00,
            "total_tips":       1720.00,
            "total_tax":        1078.00,
            "total_collected":  15200.00,
            "total_discounts":  310.00,
            "gc_redemptions":   230.00,
            "avg_ticket":       75.62,
            "mom_growth_pct":   30.2,
            "refund_count":     1,
            "cancel_count":     4,
        },
        {
            "period":           "2025-04",
            "visit_count":      195,
            "service_revenue":  13100.00,
            "total_tips":       1650.00,
            "total_tax":        1048.00,
            "total_collected":  14750.00,
            "total_discounts":  280.00,
            "gc_redemptions":   210.00,
            "avg_ticket":       75.64,
            "mom_growth_pct":   -2.8,
            "refund_count":     2,
            "cancel_count":     6,
        },
        {
            "period":           "2025-05",
            "visit_count":      210,
            "service_revenue":  14200.00,
            "total_tips":       1810.00,
            "total_tax":        1136.00,
            "total_collected":  16010.00,
            "total_discounts":  340.00,
            "gc_redemptions":   260.00,
            "avg_ticket":       76.19,
            "mom_growth_pct":   8.4,
            "refund_count":     1,
            "cancel_count":     3,
        },
        {
            "period":           "2025-06",
            "visit_count":      223,
            "service_revenue":  15300.00,
            "total_tips":       1980.00,
            "total_tax":        1224.00,
            "total_collected":  17280.00,
            "total_discounts":  370.00,
            "gc_redemptions":   290.00,
            "avg_ticket":       77.10,
            "mom_growth_pct":   7.7,
            "refund_count":     0,
            "cancel_count":     4,
        },
    ],
    "meta": {
        "total_service_revenue": 77670.00,
        "total_visits":          1170,
        "best_period":           "2025-06",
        "worst_period":          "2025-02",
        "trend_slope":           812.40,   # positive = growing
    },
}

# ── /api/v1/leo/revenue/payment-types ────────────────────────────────────────
PAYMENT_TYPES = {
    "business_id": 42,
    "data": [
        {"payment_type": "Card",     "visit_count": 748, "revenue": 52200.00, "pct_of_total": 67.2},
        {"payment_type": "Cash",     "visit_count": 289, "revenue": 18840.00, "pct_of_total": 24.3},
        {"payment_type": "GiftCard", "visit_count":  93, "revenue":  5310.00, "pct_of_total":  6.8},
        {"payment_type": "Other",    "visit_count":  40, "revenue":  1320.00, "pct_of_total":  1.7},
    ],
}

# ── /api/v1/leo/revenue/by-staff ─────────────────────────────────────────────
STAFF_REVENUE = {
    "business_id": 42,
    "data": [
        {
            "emp_id":         12,
            "staff_name":     "Maria Lopez",
            "visit_count":    318,
            "service_revenue": 22540.00,
            "tips_collected":  3180.00,
            "avg_ticket":      80.94,
            "revenue_rank":    1,
        },
        {
            "emp_id":         15,
            "staff_name":     "James Carter",
            "visit_count":    280,
            "service_revenue": 19600.00,
            "tips_collected":  2520.00,
            "avg_ticket":      78.00,
            "revenue_rank":    2,
        },
        {
            "emp_id":         9,
            "staff_name":     "Aisha Nwosu",
            "visit_count":    261,
            "service_revenue": 17940.00,
            "tips_collected":  2090.00,
            "avg_ticket":      77.16,
            "revenue_rank":    3,
        },
        {
            "emp_id":         21,
            "staff_name":     "Tom Rivera",  # no longer active — kept for history
            "visit_count":    171,
            "service_revenue": 11480.00,
            "tips_collected":  1150.00,
            "avg_ticket":      74.89,
            "revenue_rank":    4,
        },
    ],
}

# ── /api/v1/leo/revenue/by-location ──────────────────────────────────────────
LOCATION_REVENUE = {
    "business_id": 42,
    "data": [
        # Location 1 — Main St
        {"location_id": 1, "location_name": "Main St",    "period": "2025-01", "visit_count":  98, "service_revenue":  6240.00, "total_tips":  790.00, "avg_ticket": 72.00, "total_discounts": 120.00, "gc_redemptions": 100.00, "pct_of_total_revenue": 55.5, "mom_growth_pct": None},
        {"location_id": 1, "location_name": "Main St",    "period": "2025-02", "visit_count":  91, "service_revenue":  5840.00, "total_tips":  710.00, "avg_ticket": 71.36, "total_discounts": 105.00, "gc_redemptions":  80.00, "pct_of_total_revenue": 56.4, "mom_growth_pct": -6.4},
        {"location_id": 1, "location_name": "Main St",    "period": "2025-03", "visit_count": 112, "service_revenue":  7620.00, "total_tips":  980.00, "avg_ticket": 75.89, "total_discounts": 170.00, "gc_redemptions": 130.00, "pct_of_total_revenue": 56.5, "mom_growth_pct": 30.5},
        {"location_id": 1, "location_name": "Main St",    "period": "2025-04", "visit_count": 108, "service_revenue":  7380.00, "total_tips":  940.00, "avg_ticket": 75.31, "total_discounts": 155.00, "gc_redemptions": 115.00, "pct_of_total_revenue": 56.3, "mom_growth_pct": -3.1},
        {"location_id": 1, "location_name": "Main St",    "period": "2025-05", "visit_count": 116, "service_revenue":  8020.00, "total_tips": 1030.00, "avg_ticket": 76.54, "total_discounts": 190.00, "gc_redemptions": 145.00, "pct_of_total_revenue": 56.5, "mom_growth_pct":  8.7},
        {"location_id": 1, "location_name": "Main St",    "period": "2025-06", "visit_count": 123, "service_revenue":  8660.00, "total_tips": 1120.00, "avg_ticket": 77.34, "total_discounts": 205.00, "gc_redemptions": 160.00, "pct_of_total_revenue": 56.6, "mom_growth_pct":  8.0},
        # Location 2 — Westside
        {"location_id": 2, "location_name": "Westside",   "period": "2025-01", "visit_count":  80, "service_revenue":  5000.00, "total_tips":  640.50, "avg_ticket": 70.00, "total_discounts": 100.00, "gc_redemptions":  80.00, "pct_of_total_revenue": 44.5, "mom_growth_pct": None},
        {"location_id": 2, "location_name": "Westside",   "period": "2025-02", "visit_count":  72, "service_revenue":  4510.00, "total_tips":  570.00, "avg_ticket": 70.47, "total_discounts":  85.00, "gc_redemptions":  60.00, "pct_of_total_revenue": 43.6, "mom_growth_pct": -9.8},
        {"location_id": 2, "location_name": "Westside",   "period": "2025-03", "visit_count":  89, "service_revenue":  5860.00, "total_tips":  740.00, "avg_ticket": 74.94, "total_discounts": 140.00, "gc_redemptions": 100.00, "pct_of_total_revenue": 43.5, "mom_growth_pct": 30.0},
        {"location_id": 2, "location_name": "Westside",   "period": "2025-04", "visit_count":  87, "service_revenue":  5720.00, "total_tips":  710.00, "avg_ticket": 74.29, "total_discounts": 125.00, "gc_redemptions":  95.00, "pct_of_total_revenue": 43.7, "mom_growth_pct": -2.4},
        {"location_id": 2, "location_name": "Westside",   "period": "2025-05", "visit_count":  94, "service_revenue":  6180.00, "total_tips":  780.00, "avg_ticket": 74.46, "total_discounts": 150.00, "gc_redemptions": 115.00, "pct_of_total_revenue": 43.5, "mom_growth_pct":  8.0},
        {"location_id": 2, "location_name": "Westside",   "period": "2025-06", "visit_count": 100, "service_revenue":  6640.00, "total_tips":  860.00, "avg_ticket": 76.78, "total_discounts": 165.00, "gc_redemptions": 130.00, "pct_of_total_revenue": 43.4, "mom_growth_pct":  7.4},
    ],
}

# ── /api/v1/leo/revenue/promo-impact ─────────────────────────────────────────
PROMO_IMPACT = {
    "business_id": 42,
    "data": [
        {
            "promo_code":            "WELCOME10",
            "promo_description":     "10% off first visit",
            "location_id":           1,
            "location_name":         "Main St",
            "times_used":            38,
            "total_discount_given":  420.00,
            "revenue_after_discount": 3360.00,
        },
        {
            "promo_code":            "WELCOME10",
            "promo_description":     "10% off first visit",
            "location_id":           2,
            "location_name":         "Westside",
            "times_used":            27,
            "total_discount_given":  295.00,
            "revenue_after_discount": 2360.00,
        },
        {
            "promo_code":            "SUMMER20",
            "promo_description":     "$20 off summer package",
            "location_id":           1,
            "location_name":         "Main St",
            "times_used":            14,
            "total_discount_given":  280.00,
            "revenue_after_discount": 980.00,
        },
        {
            "promo_code":            "SUMMER20",
            "promo_description":     "$20 off summer package",
            "location_id":           2,
            "location_name":         "Westside",
            "times_used":            10,
            "total_discount_given":  200.00,
            "revenue_after_discount": 700.00,
        },
    ],
    "meta": {
        "total_discount_all_promos": 1195.00,
        "promo_visit_count": 89,
    },
}

# ── /api/v1/leo/revenue/failed-refunds ───────────────────────────────────────
FAILED_REFUNDS = {
    "business_id": 42,
    "data": [
        {
            "status_code":       0,
            "status_label":      "Failed",
            "visit_count":       4,
            "lost_revenue":      280.00,
            "avg_lost_per_visit": 70.00,
        },
        {
            "status_code":       4,
            "status_label":      "Refunded",
            "visit_count":       9,
            "lost_revenue":      675.00,
            "avg_lost_per_visit": 75.00,
        },
        {
            "status_code":       5,
            "status_label":      "Canceled",
            "visit_count":       29,
            "lost_revenue":      2030.00,
            "avg_lost_per_visit": 70.00,
        },
    ],
    "meta": {
        "total_lost_revenue":     2985.00,
        "total_affected_visits":  42,
    },
}

# ── Lookup: endpoint path → fixture ──────────────────────────────────────────
FIXTURES: dict[str, dict] = {
    "/api/v1/leo/revenue/monthly-summary": MONTHLY_SUMMARY,
    "/api/v1/leo/revenue/payment-types":   PAYMENT_TYPES,
    "/api/v1/leo/revenue/by-staff":        STAFF_REVENUE,
    "/api/v1/leo/revenue/by-location":     LOCATION_REVENUE,
    "/api/v1/leo/revenue/promo-impact":    PROMO_IMPACT,
    "/api/v1/leo/revenue/failed-refunds":  FAILED_REFUNDS,
}
