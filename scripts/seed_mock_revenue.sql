-- =============================================================================
-- scripts/seed_mock_revenue.sql
-- Seeds mock revenue fixture data into the warehouse for business_id = 42
-- Matches the fixture data in tests/mocks/revenue_fixtures.py exactly.
-- Run with:
--   psql -h localhost -p 5433 -U LeoWearhouseUser -d LeoWearhouseDB -f scripts/seed_mock_revenue.sql
-- =============================================================================

-- Clean up any previous test data for business_id = 42
DELETE FROM wh_monthly_revenue   WHERE business_id = 42;
DELETE FROM wh_payment_breakdown WHERE business_id = 42;
DELETE FROM wh_staff_performance WHERE business_id = 42;

-- =============================================================================
-- wh_monthly_revenue — 6 months of data, 2 locations + org rollup (location_id=0)
-- =============================================================================

-- ── Org-level rollup (location_id = 0) ───────────────────────────────────────
INSERT INTO wh_monthly_revenue
    (business_id, location_id, period_start, period_end,
     gross_revenue, total_revenue, total_tips, total_tax, total_discounts,
     total_gc_amount, visit_count, successful_visit_count,
     refunded_visit_count, cancelled_visit_count, avg_visit_value,
     cash_revenue, card_revenue, other_revenue)
VALUES
    (42, 0, '2025-01-01', '2025-01-31', 11240.00, 11240.00, 1430.50,  898.00,  220.00, 180.00, 178, 178, 2, 5,  71.14, 2810.00, 7756.00,  674.00),
    (42, 0, '2025-02-01', '2025-02-28', 10350.00, 10350.00, 1280.00,  828.00,  190.00, 140.00, 163, 163, 3, 7,  71.34, 2587.50, 7138.50,  624.00),
    (42, 0, '2025-03-01', '2025-03-31', 13480.00, 13480.00, 1720.00, 1078.00,  310.00, 230.00, 201, 201, 1, 4,  75.62, 3370.00, 9292.00,  818.00),
    (42, 0, '2025-04-01', '2025-04-30', 13100.00, 13100.00, 1650.00, 1048.00,  280.00, 210.00, 195, 195, 2, 6,  75.64, 3275.00, 9026.00,  799.00),
    (42, 0, '2025-05-01', '2025-05-31', 14200.00, 14200.00, 1810.00, 1136.00,  340.00, 260.00, 210, 210, 1, 3,  76.19, 3550.00, 9796.00,  854.00),
    (42, 0, '2025-06-01', '2025-06-30', 15300.00, 15300.00, 1980.00, 1224.00,  370.00, 290.00, 223, 223, 0, 4,  77.10, 3825.00,10557.00,  918.00)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    gross_revenue          = EXCLUDED.gross_revenue,
    total_revenue          = EXCLUDED.total_revenue,
    total_tips             = EXCLUDED.total_tips,
    total_tax              = EXCLUDED.total_tax,
    total_discounts        = EXCLUDED.total_discounts,
    total_gc_amount        = EXCLUDED.total_gc_amount,
    visit_count            = EXCLUDED.visit_count,
    successful_visit_count = EXCLUDED.successful_visit_count,
    refunded_visit_count   = EXCLUDED.refunded_visit_count,
    cancelled_visit_count  = EXCLUDED.cancelled_visit_count,
    avg_visit_value        = EXCLUDED.avg_visit_value,
    cash_revenue           = EXCLUDED.cash_revenue,
    card_revenue           = EXCLUDED.card_revenue,
    other_revenue          = EXCLUDED.other_revenue,
    updated_at             = now();

-- ── Location 1 — Main St ──────────────────────────────────────────────────────
INSERT INTO wh_monthly_revenue
    (business_id, location_id, period_start, period_end,
     gross_revenue, total_revenue, total_tips, total_tax, total_discounts,
     total_gc_amount, visit_count, successful_visit_count,
     refunded_visit_count, cancelled_visit_count, avg_visit_value,
     cash_revenue, card_revenue, other_revenue)
VALUES
    (42, 1, '2025-01-01', '2025-01-31', 6240.00, 6240.00,  790.00,  499.00, 120.00, 100.00,  98,  98, 1, 2, 72.00, 1560.00, 4304.00, 376.00),
    (42, 1, '2025-02-01', '2025-02-28', 5840.00, 5840.00,  710.00,  467.00, 105.00,  80.00,  91,  91, 2, 4, 71.36, 1460.00, 4030.00, 350.00),
    (42, 1, '2025-03-01', '2025-03-31', 7620.00, 7620.00,  980.00,  610.00, 170.00, 130.00, 112, 112, 0, 2, 75.89, 1905.00, 5258.00, 457.00),
    (42, 1, '2025-04-01', '2025-04-30', 7380.00, 7380.00,  940.00,  590.00, 155.00, 115.00, 108, 108, 1, 3, 75.31, 1845.00, 5092.00, 443.00),
    (42, 1, '2025-05-01', '2025-05-31', 8020.00, 8020.00, 1030.00,  642.00, 190.00, 145.00, 116, 116, 1, 2, 76.54, 2005.00, 5534.00, 481.00),
    (42, 1, '2025-06-01', '2025-06-30', 8660.00, 8660.00, 1120.00,  693.00, 205.00, 160.00, 123, 123, 0, 2, 77.34, 2165.00, 5976.00, 519.00)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    gross_revenue          = EXCLUDED.gross_revenue,
    total_revenue          = EXCLUDED.total_revenue,
    total_tips             = EXCLUDED.total_tips,
    updated_at             = now();

-- ── Location 2 — Westside ─────────────────────────────────────────────────────
INSERT INTO wh_monthly_revenue
    (business_id, location_id, period_start, period_end,
     gross_revenue, total_revenue, total_tips, total_tax, total_discounts,
     total_gc_amount, visit_count, successful_visit_count,
     refunded_visit_count, cancelled_visit_count, avg_visit_value,
     cash_revenue, card_revenue, other_revenue)
VALUES
    (42, 2, '2025-01-01', '2025-01-31', 5000.00, 5000.00,  640.50,  400.00, 100.00,  80.00,  80,  80, 1, 3, 70.00, 1250.00, 3450.00, 300.00),
    (42, 2, '2025-02-01', '2025-02-28', 4510.00, 4510.00,  570.00,  361.00,  85.00,  60.00,  72,  72, 1, 3, 70.47, 1127.50, 3112.00, 270.50),
    (42, 2, '2025-03-01', '2025-03-31', 5860.00, 5860.00,  740.00,  469.00, 140.00, 100.00,  89,  89, 1, 2, 74.94, 1465.00, 4034.00, 361.00),
    (42, 2, '2025-04-01', '2025-04-30', 5720.00, 5720.00,  710.00,  458.00, 125.00,  95.00,  87,  87, 1, 3, 74.29, 1430.00, 3947.00, 343.00),
    (42, 2, '2025-05-01', '2025-05-31', 6180.00, 6180.00,  780.00,  494.00, 150.00, 115.00,  94,  94, 0, 1, 74.46, 1545.00, 4264.00, 371.00),
    (42, 2, '2025-06-01', '2025-06-30', 6640.00, 6640.00,  860.00,  531.00, 165.00, 130.00, 100, 100, 0, 2, 76.78, 1660.00, 4582.00, 398.00)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    gross_revenue          = EXCLUDED.gross_revenue,
    total_revenue          = EXCLUDED.total_revenue,
    total_tips             = EXCLUDED.total_tips,
    updated_at             = now();

-- =============================================================================
-- wh_payment_breakdown — org-level payment type split
-- =============================================================================
INSERT INTO wh_payment_breakdown
    (business_id, location_id, period_start, period_end,
     cash_amount, cash_count, card_amount, card_count,
     gift_card_amount, gift_card_count, other_amount, other_count,
     total_amount, total_count)
VALUES
    (42, 0, '2025-01-01', '2025-06-30',
     18840.00, 289, 52200.00, 748, 5310.00, 93, 1320.00, 40,
     77670.00, 1170)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    cash_amount      = EXCLUDED.cash_amount,
    card_amount      = EXCLUDED.card_amount,
    gift_card_amount = EXCLUDED.gift_card_amount,
    other_amount     = EXCLUDED.other_amount,
    total_amount     = EXCLUDED.total_amount,
    updated_at       = now();

-- =============================================================================
-- wh_staff_performance — 4 staff members, 6-month window
-- =============================================================================
INSERT INTO wh_staff_performance
    (business_id, employee_id, employee_name, period_start, period_end,
     total_visits, total_revenue, total_tips, total_commission,
     appointments_booked, appointments_completed, appointments_cancelled,
     avg_rating, review_count, utilisation_rate)
VALUES
    (42, 12, 'Maria Lopez',  '2025-01-01', '2025-06-30', 318, 22540.00, 3180.00, 2254.00, 330, 318, 12, 4.80, 95, 82.00),
    (42, 15, 'James Carter', '2025-01-01', '2025-06-30', 280, 19600.00, 2520.00, 1960.00, 290, 280, 10, 4.60, 78, 76.00),
    (42,  9, 'Aisha Nwosu',  '2025-01-01', '2025-06-30', 261, 17940.00, 2090.00, 1794.00, 272, 261, 11, 4.70, 71, 74.00),
    (42, 21, 'Tom Rivera',   '2025-01-01', '2025-06-30', 171, 11480.00, 1150.00, 1148.00, 178, 171,  7, 4.20, 42, 65.00)
ON CONFLICT (business_id, employee_id, period_start) DO UPDATE SET
    total_visits    = EXCLUDED.total_visits,
    total_revenue   = EXCLUDED.total_revenue,
    total_tips      = EXCLUDED.total_tips,
    avg_rating      = EXCLUDED.avg_rating,
    updated_at      = now();

-- =============================================================================
-- Verify the seed
-- =============================================================================
SELECT 'wh_monthly_revenue'   AS table_name, COUNT(*) AS rows FROM wh_monthly_revenue   WHERE business_id = 42
UNION ALL
SELECT 'wh_payment_breakdown' AS table_name, COUNT(*) AS rows FROM wh_payment_breakdown WHERE business_id = 42
UNION ALL
SELECT 'wh_staff_performance' AS table_name, COUNT(*) AS rows FROM wh_staff_performance WHERE business_id = 42;
