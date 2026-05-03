-- =============================================================================
-- Analytics warehouse schema (PostgreSQL)
-- =============================================================================
-- Denormalized tables populated by ETL from production MySQL. Read by
-- warehouse_client, document generator, and downstream RAG/pgvector flows.
-- No foreign keys between warehouse tables. Use UNIQUE constraints with
-- ON CONFLICT ... DO UPDATE in ETL for idempotent upserts.
--
-- Cleanup history:
--   2026-05-03: Dropped 9 pre-domain-refactor tables superseded by domain-
--               prefixed replacements (wh_appt_*, wh_svc_*, wh_staff_*_monthly,
--               wh_client_*, wh_mrk_*, wh_exp_*). Removed duplicated wh_svc_*
--               block. Final count: 48 tables (47 domain + wh_etl_log).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- wh_monthly_revenue — Monthly revenue KPIs per business (and location rollup).
-- Source: tbl_visit (Payment, Tips, Tax, Discount, TotalPay, GCAmount, PaymentType,
--         PaymentStatus, OrganizationId, RecDateTime, LocationID).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_monthly_revenue (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_tips DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_tax DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_discounts DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_gc_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    gross_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    visit_count INTEGER NOT NULL DEFAULT 0,
    successful_visit_count INTEGER NOT NULL DEFAULT 0,
    refunded_visit_count INTEGER NOT NULL DEFAULT 0,
    cancelled_visit_count INTEGER NOT NULL DEFAULT 0,
    avg_visit_value DECIMAL(15, 2) NOT NULL DEFAULT 0,
    cash_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    card_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    other_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_monthly_revenue_dim UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_monthly_revenue_business_id ON wh_monthly_revenue (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_monthly_revenue_business_period ON wh_monthly_revenue (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_daily_revenue — Daily revenue KPIs (trends, day-over-day).
-- Source: tbl_visit (same measures as monthly, grouped by calendar day).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_daily_revenue (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    revenue_date DATE NOT NULL,
    total_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_tips DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_tax DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_discounts DECIMAL(15, 2) NOT NULL DEFAULT 0,
    gross_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    visit_count INTEGER NOT NULL DEFAULT 0,
    successful_visit_count INTEGER NOT NULL DEFAULT 0,
    avg_visit_value DECIMAL(15, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_daily_revenue_dim UNIQUE (business_id, location_id, revenue_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_daily_revenue_business_id ON wh_daily_revenue (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_daily_revenue_business_revenue_date ON wh_daily_revenue (business_id, revenue_date);

-- -----------------------------------------------------------------------------
-- wh_payment_breakdown — Monthly totals by payment method (cash, card, GC, other).
-- Source: tbl_visit.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_payment_breakdown (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    cash_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    cash_count INTEGER NOT NULL DEFAULT 0,
    card_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    card_count INTEGER NOT NULL DEFAULT 0,
    gift_card_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    gift_card_count INTEGER NOT NULL DEFAULT 0,
    other_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    other_count INTEGER NOT NULL DEFAULT 0,
    total_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_payment_breakdown_dim UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_payment_breakdown_business_id ON wh_payment_breakdown (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_payment_breakdown_business_period ON wh_payment_breakdown (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_etl_log — ETL run history (monitoring, debugging, row counts).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_etl_log (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL DEFAULT gen_random_uuid(),
    target_table VARCHAR(100) NOT NULL,
    business_id INTEGER,
    period_start DATE,
    period_end DATE,
    status VARCHAR(20) NOT NULL,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_deleted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    duration_seconds DECIMAL(10, 3),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wh_etl_log_target_started ON wh_etl_log (target_table, started_at);
CREATE INDEX IF NOT EXISTS idx_wh_etl_log_status_started ON wh_etl_log (status, started_at);



-- =============================================================================
-- APPOINTMENTS DOMAIN — Sprint 2
-- =============================================================================
-- 4 tables covering monthly funnel, staff breakdown, service breakdown,
-- and the staff×service cross.
-- (Pre-refactor wh_appointment_metrics was dropped in the 2026-05-03 cleanup.)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- wh_appt_monthly_summary
-- Monthly appointment funnel per location + org rollup (location_id = 0).
-- Includes time slots, duration, MoM growth, and rollup flag.
-- Source: analytics backend /api/v1/leo/appointments/monthly-summary
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_appt_monthly_summary (
    id                      BIGSERIAL PRIMARY KEY,
    business_id             INTEGER       NOT NULL,
    location_id             INTEGER       NOT NULL DEFAULT 0,
    location_name           VARCHAR(150)  NOT NULL DEFAULT '',
    location_city           VARCHAR(100)  NOT NULL DEFAULT '',
    period_start            DATE          NOT NULL,
    period_end              DATE          NOT NULL,
    is_rollup               BOOLEAN       NOT NULL DEFAULT FALSE,
    total_booked            INTEGER       NOT NULL DEFAULT 0,
    confirmed_count         INTEGER       NOT NULL DEFAULT 0,
    completed_count         INTEGER       NOT NULL DEFAULT 0,
    cancelled_count         INTEGER       NOT NULL DEFAULT 0,
    no_show_count           INTEGER       NOT NULL DEFAULT 0,
    morning_count           INTEGER       NOT NULL DEFAULT 0,
    afternoon_count         INTEGER       NOT NULL DEFAULT 0,
    evening_count           INTEGER       NOT NULL DEFAULT 0,
    weekend_count           INTEGER       NOT NULL DEFAULT 0,
    weekday_count           INTEGER       NOT NULL DEFAULT 0,
    avg_actual_duration_min DECIMAL(6,1),
    cancellation_rate_pct   DECIMAL(5,2)  NOT NULL DEFAULT 0,
    no_show_rate_pct        DECIMAL(5,2)  NOT NULL DEFAULT 0,
    mom_growth_pct          DECIMAL(6,2),
    walkin_count            INTEGER       NOT NULL DEFAULT 0,
    app_booking_count       INTEGER       NOT NULL DEFAULT 0,
    peak_slot               VARCHAR(20),
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_appt_monthly_summary UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_appt_monthly_summary_business_id
    ON wh_appt_monthly_summary (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_appt_monthly_summary_business_period
    ON wh_appt_monthly_summary (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_appt_monthly_summary_location
    ON wh_appt_monthly_summary (business_id, location_id, period_start);


-- -----------------------------------------------------------------------------
-- wh_appt_staff_breakdown
-- Monthly appointment counts per staff member per location.
-- Source: analytics backend /api/v1/leo/appointments/by-staff
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_appt_staff_breakdown (
    id                          BIGSERIAL PRIMARY KEY,
    business_id                 INTEGER       NOT NULL,
    staff_id                    INTEGER       NOT NULL,
    staff_name                  VARCHAR(150)  NOT NULL DEFAULT '',
    location_id                 INTEGER       NOT NULL DEFAULT 0,
    location_name               VARCHAR(150)  NOT NULL DEFAULT '',
    period_start                DATE          NOT NULL,
    period_end                  DATE          NOT NULL,
    total_booked                INTEGER       NOT NULL DEFAULT 0,
    completed_count             INTEGER       NOT NULL DEFAULT 0,
    completion_rate_pct         DECIMAL(5,2)  NOT NULL DEFAULT 0,
    cancelled_count             INTEGER       NOT NULL DEFAULT 0,
    no_show_count               INTEGER       NOT NULL DEFAULT 0,
    no_show_rate_pct            DECIMAL(5,2)  NOT NULL DEFAULT 0,
    distinct_services_handled   INTEGER       NOT NULL DEFAULT 0,
    mom_growth_pct              DECIMAL(6,2),
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_appt_staff_breakdown UNIQUE (business_id, staff_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_breakdown_business_id
    ON wh_appt_staff_breakdown (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_breakdown_business_period
    ON wh_appt_staff_breakdown (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_breakdown_staff
    ON wh_appt_staff_breakdown (business_id, staff_id);


-- -----------------------------------------------------------------------------
-- wh_appt_service_breakdown
-- Monthly appointment counts per service type.
-- Source: analytics backend /api/v1/leo/appointments/by-service
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_appt_service_breakdown (
    id                          BIGSERIAL PRIMARY KEY,
    business_id                 INTEGER       NOT NULL,
    service_id                  INTEGER       NOT NULL,
    service_name                VARCHAR(200)  NOT NULL DEFAULT '',
    period_start                DATE          NOT NULL,
    period_end                  DATE          NOT NULL,
    total_booked                INTEGER       NOT NULL DEFAULT 0,
    completed_count             INTEGER       NOT NULL DEFAULT 0,
    cancelled_count             INTEGER       NOT NULL DEFAULT 0,
    distinct_clients            INTEGER       NOT NULL DEFAULT 0,
    repeat_visit_count          INTEGER       NOT NULL DEFAULT 0,
    avg_scheduled_duration_min  DECIMAL(6,1),
    avg_actual_duration_min     DECIMAL(6,1),
    cancellation_rate_pct       DECIMAL(5,2)  NOT NULL DEFAULT 0,
    morning_count               INTEGER       NOT NULL DEFAULT 0,
    afternoon_count             INTEGER       NOT NULL DEFAULT 0,
    evening_count               INTEGER       NOT NULL DEFAULT 0,
    peak_slot                   VARCHAR(20),
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_appt_service_breakdown UNIQUE (business_id, service_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_appt_service_breakdown_business_id
    ON wh_appt_service_breakdown (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_appt_service_breakdown_business_period
    ON wh_appt_service_breakdown (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_appt_service_breakdown_service
    ON wh_appt_service_breakdown (business_id, service_id);


-- -----------------------------------------------------------------------------
-- wh_appt_staff_service_cross
-- Monthly appointment counts per staff member per service type.
-- Source: analytics backend /api/v1/leo/appointments/staff-service-cross
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_appt_staff_service_cross (
    id                  BIGSERIAL PRIMARY KEY,
    business_id         INTEGER       NOT NULL,
    staff_id            INTEGER       NOT NULL,
    staff_name          VARCHAR(150)  NOT NULL DEFAULT '',
    service_id          INTEGER       NOT NULL,
    service_name        VARCHAR(200)  NOT NULL DEFAULT '',
    period_start        DATE          NOT NULL,
    period_end          DATE          NOT NULL,
    total_booked        INTEGER       NOT NULL DEFAULT 0,
    completed_count     INTEGER       NOT NULL DEFAULT 0,
    completion_rate_pct DECIMAL(5,2)  NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_appt_staff_service_cross UNIQUE (business_id, staff_id, service_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_service_cross_business_id
    ON wh_appt_staff_service_cross (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_service_cross_business_period
    ON wh_appt_staff_service_cross (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_service_cross_staff
    ON wh_appt_staff_service_cross (business_id, staff_id);
CREATE INDEX IF NOT EXISTS idx_wh_appt_staff_service_cross_service
    ON wh_appt_staff_service_cross (business_id, service_id);



-- =============================================================================
-- STAFF PERFORMANCE DOMAIN — Sprint 3
-- =============================================================================
-- 3 tables:
--   wh_staff_performance_monthly  — KPIs per staff × location × period
--   wh_staff_summary              — all-time totals per staff
--   wh_staff_attendance           — hours worked per staff × location × period
-- (Pre-refactor wh_staff_performance and wh_attendance_summary were dropped
--  in the 2026-05-03 cleanup.)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- wh_staff_performance_monthly
-- Monthly staff KPIs: revenue, visits, commission, ratings, cancellations.
-- Grain: one row per (business_id, employee_id, location_id, period_start).
-- Source: analytics backend POST /api/v1/leo/staff-performance (mode=monthly)
-- Covers test questions: Q1–Q8, Q11–Q22, Q25–Q32, Q34–Q35, Q37–Q39
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_staff_performance_monthly (
    id                          BIGSERIAL PRIMARY KEY,
    business_id                 INTEGER         NOT NULL,

    -- Staff identifiers
    employee_id                 INTEGER         NOT NULL,
    employee_name               VARCHAR(150)    NOT NULL DEFAULT '',
    employee_first_name         VARCHAR(75)     NOT NULL DEFAULT '',
    employee_last_name          VARCHAR(75)     NOT NULL DEFAULT '',
    is_active                   BOOLEAN         NOT NULL DEFAULT TRUE,
    hire_date                   DATE,

    -- Location (where visits occurred — not staff home location)
    location_id                 INTEGER         NOT NULL DEFAULT 0,
    location_name               VARCHAR(150)    NOT NULL DEFAULT '',

    -- Period
    period_start                DATE            NOT NULL,
    period_end                  DATE            NOT NULL,

    -- Visit metrics (PaymentStatus = 1 — successful only)
    completed_visit_count       INTEGER         NOT NULL DEFAULT 0,
    unique_customer_count       INTEGER         NOT NULL DEFAULT 0,

    -- Revenue metrics
    revenue                     DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    tips                        DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    total_pay                   DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    avg_revenue_per_visit       DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    commission_earned           DECIMAL(15, 2)  NOT NULL DEFAULT 0,

    -- Cancelled / refunded payment counts (from tbl_visit PaymentStatus)
    -- Note: these are payment-level cancellations, not appointment-level.
    -- Appointment-level no-shows live in wh_appt_staff_breakdown.
    cancelled_payment_count     INTEGER         NOT NULL DEFAULT 0,
    refunded_payment_count      INTEGER         NOT NULL DEFAULT 0,
    revoked_payment_count       INTEGER         NOT NULL DEFAULT 0,

    -- Rating metrics (from tbl_emp_reviews — staff-specific reviews only)
    -- avg_rating is NULL when review_count = 0 (not 0 — avoids misleading "zero" rating)
    review_count                INTEGER         NOT NULL DEFAULT 0,
    avg_rating                  DECIMAL(3, 2),

    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_wh_staff_performance_monthly
        UNIQUE (business_id, employee_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_staff_perf_monthly_business
    ON wh_staff_performance_monthly (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_staff_perf_monthly_business_period
    ON wh_staff_performance_monthly (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_staff_perf_monthly_employee
    ON wh_staff_performance_monthly (business_id, employee_id);
CREATE INDEX IF NOT EXISTS idx_wh_staff_perf_monthly_location
    ON wh_staff_performance_monthly (business_id, location_id, period_start);
-- Active staff filter (common query: WHERE is_active = TRUE)
CREATE INDEX IF NOT EXISTS idx_wh_staff_perf_monthly_active
    ON wh_staff_performance_monthly (business_id, is_active, period_start);


-- -----------------------------------------------------------------------------
-- wh_staff_summary
-- All-time / YTD aggregated staff KPIs — one row per staff member.
-- Grain: one row per (business_id, employee_id).
-- Source: analytics backend POST /api/v1/leo/staff-performance (mode=summary)
-- Covers test questions: Q9 (rank by revenue), Q10 (lowest rating), Q29, Q31
--
-- Updated on every ETL run (ON CONFLICT DO UPDATE replaces the row).
-- The period_from / period_to columns document what date range was aggregated.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_staff_summary (
    id                              BIGSERIAL PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,

    -- Staff identifiers
    employee_id                     INTEGER         NOT NULL,
    employee_name                   VARCHAR(150)    NOT NULL DEFAULT '',
    employee_first_name             VARCHAR(75)     NOT NULL DEFAULT '',
    employee_last_name              VARCHAR(75)     NOT NULL DEFAULT '',
    is_active                       BOOLEAN         NOT NULL DEFAULT TRUE,
    hire_date                       DATE,

    -- Aggregation window
    period_from                     VARCHAR(7),     -- 'YYYY-MM' of first active period
    period_to                       VARCHAR(7),     -- 'YYYY-MM' of last active period

    -- YTD / all-time totals
    total_visits_ytd                INTEGER         NOT NULL DEFAULT 0,
    total_revenue_ytd               DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    total_tips_ytd                  DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    total_commission_ytd            DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    total_customers_served          INTEGER         NOT NULL DEFAULT 0,
    total_cancelled_ytd             INTEGER         NOT NULL DEFAULT 0,
    total_refunded_ytd              INTEGER         NOT NULL DEFAULT 0,

    -- Rating aggregates
    -- overall_avg_rating is NULL when total_review_count = 0
    overall_avg_rating              DECIMAL(3, 2),
    total_review_count              INTEGER         NOT NULL DEFAULT 0,

    -- Derived metrics
    lifetime_avg_revenue_per_visit  DECIMAL(15, 2)  NOT NULL DEFAULT 0,

    -- Revenue share in most recent period (for Q35 — % of org revenue)
    -- NULL for inactive staff with no activity in the latest period
    revenue_pct_of_org_latest       DECIMAL(6, 2),

    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_wh_staff_summary
        UNIQUE (business_id, employee_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_staff_summary_business
    ON wh_staff_summary (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_staff_summary_employee
    ON wh_staff_summary (business_id, employee_id);
-- Revenue ranking index (Q9: rank all staff by revenue)
CREATE INDEX IF NOT EXISTS idx_wh_staff_summary_revenue_rank
    ON wh_staff_summary (business_id, total_revenue_ytd DESC);
-- Rating ranking index (Q10: lowest rating)
CREATE INDEX IF NOT EXISTS idx_wh_staff_summary_rating
    ON wh_staff_summary (business_id, overall_avg_rating ASC NULLS LAST);


-- -----------------------------------------------------------------------------
-- wh_staff_attendance
-- Monthly attendance hours per staff member per location.
-- Grain: one row per (business_id, employee_id, location_id, period_start).
-- Source: analytics backend POST /api/v1/leo/staff-attendance
-- Covers test questions: Q33 (who clocked the most hours)
--
-- Time format confirmed by team (2026-04-13): '10:44:15 PM'.
-- No-time value '0' → excluded from duration calc, counted in days_missing_signout.
-- Overnight shifts handled server-side. Duration capped at 24h/day.
-- days_missing_signout is a data quality indicator, not an error.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_staff_attendance (
    id                      BIGSERIAL PRIMARY KEY,
    business_id             INTEGER         NOT NULL,

    -- Staff identifiers
    employee_id             INTEGER         NOT NULL,
    employee_name           VARCHAR(150)    NOT NULL DEFAULT '',
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,

    -- Location where attendance was recorded
    location_id             INTEGER         NOT NULL DEFAULT 0,
    location_name           VARCHAR(150)    NOT NULL DEFAULT '',

    -- Period
    period_start            DATE            NOT NULL,
    period_end              DATE            NOT NULL,

    -- Attendance counts
    -- days_with_signin:     signed in at least once (time_sign_in != '0')
    -- days_fully_recorded:  both sign-in and sign-out recorded — hours denominator
    -- days_missing_signout: signed in but no sign-out — data quality flag
    days_with_signin        INTEGER         NOT NULL DEFAULT 0,
    days_fully_recorded     INTEGER         NOT NULL DEFAULT 0,
    days_missing_signout    INTEGER         NOT NULL DEFAULT 0,

    -- Hours worked (computed server-side from varchar time strings)
    -- total_hours_worked: sum of durations for fully-recorded days only
    -- avg_hours_per_day:  total_hours / days_fully_recorded (NULL if 0 days)
    total_hours_worked      DECIMAL(8, 2)   NOT NULL DEFAULT 0,
    avg_hours_per_day       DECIMAL(6, 2),  -- NULL when days_fully_recorded = 0

    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_wh_staff_attendance
        UNIQUE (business_id, employee_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_staff_attendance_business
    ON wh_staff_attendance (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_staff_attendance_business_period
    ON wh_staff_attendance (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_staff_attendance_employee
    ON wh_staff_attendance (business_id, employee_id);
-- Hours ranking index (Q33: who clocked the most hours)
CREATE INDEX IF NOT EXISTS idx_wh_staff_attendance_hours_rank
    ON wh_staff_attendance (business_id, period_start, total_hours_worked DESC);



-- =============================================================================
-- SERVICES DOMAIN — Sprint 4
-- =============================================================================
-- 5 tables matching the 5 API endpoints / query sets.
-- (Pre-refactor wh_service_performance was dropped in the 2026-05-03 cleanup.)
-- =============================================================================

-- EP1: Service Monthly Summary (performed/paid side)
CREATE TABLE IF NOT EXISTS wh_svc_monthly_summary (
    id                          SERIAL PRIMARY KEY,
    business_id                 INTEGER NOT NULL,
    service_id                  INTEGER NOT NULL,
    service_name                TEXT NOT NULL,
    category_name               TEXT,
    location_id                 INTEGER NOT NULL,
    location_name               TEXT NOT NULL,
    period_start                DATE NOT NULL,
    performed_count             INTEGER NOT NULL DEFAULT 0,
    distinct_clients            INTEGER NOT NULL DEFAULT 0,
    repeat_visit_proxy          INTEGER NOT NULL DEFAULT 0,
    total_revenue               NUMERIC(12,2) NOT NULL DEFAULT 0,
    avg_charged_price           NUMERIC(10,2) NOT NULL DEFAULT 0,
    total_emp_commission        NUMERIC(12,2) NOT NULL DEFAULT 0,
    gross_margin                NUMERIC(12,2) NOT NULL DEFAULT 0,
    commission_pct_of_revenue   NUMERIC(5,1),
    mom_revenue_growth_pct      NUMERIC(6,1),
    revenue_rank                INTEGER,
    margin_rank                 INTEGER,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_svc_monthly UNIQUE (business_id, service_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_svc_monthly_biz_period
    ON wh_svc_monthly_summary (business_id, period_start);

-- EP2: Service Booking Stats (booking side)
CREATE TABLE IF NOT EXISTS wh_svc_booking_stats (
    id                          SERIAL PRIMARY KEY,
    business_id                 INTEGER NOT NULL,
    service_id                  INTEGER NOT NULL,
    service_name                TEXT NOT NULL,
    location_id                 INTEGER NOT NULL,
    location_name               TEXT NOT NULL,
    period_start                DATE NOT NULL,
    total_booked                INTEGER NOT NULL DEFAULT 0,
    completed_count             INTEGER NOT NULL DEFAULT 0,
    cancelled_count             INTEGER NOT NULL DEFAULT 0,
    no_show_count               INTEGER NOT NULL DEFAULT 0,
    cancellation_rate_pct       NUMERIC(5,1),
    avg_actual_duration_min     NUMERIC(6,1),
    distinct_clients            INTEGER NOT NULL DEFAULT 0,
    morning_bookings            INTEGER NOT NULL DEFAULT 0,
    afternoon_bookings          INTEGER NOT NULL DEFAULT 0,
    evening_bookings            INTEGER NOT NULL DEFAULT 0,
    mom_bookings_growth_pct     NUMERIC(6,1),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_svc_booking UNIQUE (business_id, service_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_svc_booking_biz_period
    ON wh_svc_booking_stats (business_id, period_start);

-- EP3: Service × Staff Matrix
CREATE TABLE IF NOT EXISTS wh_svc_staff_matrix (
    id                  SERIAL PRIMARY KEY,
    business_id         INTEGER NOT NULL,
    service_id          INTEGER NOT NULL,
    service_name        TEXT NOT NULL,
    staff_id            INTEGER NOT NULL,
    staff_name          TEXT NOT NULL,
    period_start        DATE NOT NULL,
    performed_count     INTEGER NOT NULL DEFAULT 0,
    revenue             NUMERIC(12,2) NOT NULL DEFAULT 0,
    commission_paid     NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_svc_staff UNIQUE (business_id, service_id, staff_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_svc_staff_biz_period
    ON wh_svc_staff_matrix (business_id, period_start);

-- EP4: Service Co-occurrence
CREATE TABLE IF NOT EXISTS wh_svc_co_occurrence (
    id                      SERIAL PRIMARY KEY,
    business_id             INTEGER NOT NULL,
    period_start            DATE NOT NULL,
    service_a_id            INTEGER NOT NULL,
    service_a_name          TEXT NOT NULL,
    service_b_id            INTEGER NOT NULL,
    service_b_name          TEXT NOT NULL,
    co_occurrence_count     INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_svc_cooccur UNIQUE (business_id, service_a_id, service_b_id, period_start)
);

-- EP5: Service Catalog Snapshot
CREATE TABLE IF NOT EXISTS wh_svc_catalog (
    id                              SERIAL PRIMARY KEY,
    business_id                     INTEGER NOT NULL,
    service_id                      INTEGER NOT NULL,
    service_name                    TEXT NOT NULL,
    category_name                   TEXT,
    list_price                      NUMERIC(10,2) NOT NULL DEFAULT 0,
    default_commission_rate         NUMERIC(5,2),
    commission_type                 VARCHAR(1) NOT NULL DEFAULT '%',
    scheduled_duration_min          INTEGER NOT NULL DEFAULT 0,
    is_active                       BOOLEAN NOT NULL DEFAULT true,
    created_at                      TIMESTAMPTZ,
    home_location_id                INTEGER,
    last_sold_date                  TIMESTAMPTZ,
    days_since_last_sale            INTEGER,
    lifetime_performed_count        INTEGER NOT NULL DEFAULT 0,
    new_client_first_service_count  INTEGER NOT NULL DEFAULT 0,
    dormant_flag                    BOOLEAN NOT NULL DEFAULT false,
    is_new_this_year                BOOLEAN NOT NULL DEFAULT false,
    avg_discount_pct                NUMERIC(5,1),
    scheduled_vs_actual_delta_min   NUMERIC(5,1),
    refreshed_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_svc_catalog UNIQUE (business_id, service_id)
);

CREATE INDEX IF NOT EXISTS idx_svc_catalog_biz
    ON wh_svc_catalog (business_id);



-- =============================================================================
-- CLIENTS DOMAIN — Sprint 5
-- =============================================================================
-- 3 tables support the 23 acceptance questions from the Clients sprint.
-- (Pre-refactor wh_client_metrics was dropped in the 2026-05-03 cleanup.)
--
-- Key design decisions (from Step 2 + Step 3 specs):
--   1. wh_client_retention is HISTORICAL (PK includes period) — required for
--      cohort retention (Option A). Successive period ETL runs stack.
--   2. No first_name / last_name columns — PII never lands here from the
--      RAG path. Ops-tools CSV exports use a separate (out of scope) table.
--   3. is_new_in_period / is_returning_in_period / is_reactivated_in_period
--      are stored as booleans — can be aggregated by doc generator.
--   4. days_since_last_visit is numeric (not boolean) — churn threshold
--      evaluated at doc-generation time per Step 2 risk #4.
--
-- Storage profile: ~1000 clients × 24 months × 38 business_ids ≈ 900K rows.
-- Negligible for Postgres.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Table 1 — wh_client_retention (per-client per-period snapshot)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_client_retention (
    business_id                 INTEGER NOT NULL,
    client_id                   INTEGER NOT NULL,
    period                      DATE    NOT NULL,   -- YYYY-MM-01

    -- Demographic (age derived from DOB; DOB not stored)
    age                         INTEGER,
    age_bracket                 VARCHAR(16),        -- under_25 | 25_to_40 | 40_to_55 | 55_plus | unknown
    points                      DOUBLE PRECISION,

    -- Behavioural dates
    first_visit_ever_date       DATE,
    last_visit_date             DATE,
    days_since_last_visit       INTEGER,            -- numeric, threshold applied downstream

    -- Visit counts
    total_visits_ever           INTEGER DEFAULT 0,
    visits_in_period            INTEGER DEFAULT 0,

    -- Revenue
    lifetime_revenue            DOUBLE PRECISION DEFAULT 0,
    lifetime_tips               DOUBLE PRECISION DEFAULT 0,
    lifetime_total_paid         DOUBLE PRECISION DEFAULT 0,
    revenue_in_period           DOUBLE PRECISION DEFAULT 0,
    avg_ticket                  DOUBLE PRECISION,

    -- Location attribution
    home_location_id            INTEGER,
    home_location_name          VARCHAR(100),
    first_visit_location_id     INTEGER,
    first_visit_location_name   VARCHAR(100),

    -- Status flags
    is_not_deleted              BOOLEAN NOT NULL DEFAULT TRUE,
    is_reachable_email          BOOLEAN NOT NULL DEFAULT FALSE,
    is_reachable_sms            BOOLEAN NOT NULL DEFAULT FALSE,
    is_member                   BOOLEAN NOT NULL DEFAULT FALSE,
    is_new_in_period            BOOLEAN NOT NULL DEFAULT FALSE,
    is_returning_in_period      BOOLEAN NOT NULL DEFAULT FALSE,
    is_reactivated_in_period    BOOLEAN NOT NULL DEFAULT FALSE,
    at_risk_flag                BOOLEAN NOT NULL DEFAULT FALSE,

    -- Ranks (within business, within period)
    ltv_rank                    INTEGER,
    frequency_rank              INTEGER,
    points_rank                 INTEGER,
    ltv_percentile_decile       INTEGER,            -- 1 = top 10%, 10 = bottom 10%

    etl_run_at                  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (business_id, client_id, period)
);

CREATE INDEX IF NOT EXISTS idx_wh_client_retention_biz_period
    ON wh_client_retention (business_id, period);
CREATE INDEX IF NOT EXISTS idx_wh_client_retention_biz_ltv
    ON wh_client_retention (business_id, lifetime_revenue DESC);
CREATE INDEX IF NOT EXISTS idx_wh_client_retention_biz_risk
    ON wh_client_retention (business_id, at_risk_flag)
    WHERE at_risk_flag = TRUE;


-- ─────────────────────────────────────────────────────────────────────────────
-- Table 2 — wh_client_cohort_monthly (per-period aggregate with MoM + retention)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_client_cohort_monthly (
    business_id                 INTEGER NOT NULL,
    period                      DATE    NOT NULL,   -- YYYY-MM-01

    -- Counts
    clients_total               INTEGER DEFAULT 0,
    new_clients                 INTEGER DEFAULT 0,
    returning_clients           INTEGER DEFAULT 0,
    reactivated_clients         INTEGER DEFAULT 0,
    active_clients_in_period    INTEGER DEFAULT 0,
    at_risk_clients             INTEGER DEFAULT 0,
    active_members              INTEGER DEFAULT 0,
    reachable_email             INTEGER DEFAULT 0,
    reachable_sms               INTEGER DEFAULT 0,

    -- Revenue
    total_revenue_in_period     DOUBLE PRECISION DEFAULT 0,

    -- Unique visitors (Q23 dedup)
    unique_visitors_in_period   INTEGER DEFAULT 0,

    -- MoM lookbacks (from prior period's row)
    prev_new_clients            INTEGER,
    prev_at_risk_clients        INTEGER,
    prev_active_clients         INTEGER,

    -- Derived percentages (NULL when denominator=0)
    new_clients_mom_pct         DOUBLE PRECISION,
    at_risk_mom_pct             DOUBLE PRECISION,
    new_vs_returning_split      DOUBLE PRECISION,
    retention_rate_pct          DOUBLE PRECISION,   -- cohort retention, Option A
    churn_rate_pct              DOUBLE PRECISION,
    member_overlap_pct          DOUBLE PRECISION,
    top10pct_revenue_share      DOUBLE PRECISION,

    etl_run_at                  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (business_id, period)
);

CREATE INDEX IF NOT EXISTS idx_wh_client_cohort_biz
    ON wh_client_cohort_monthly (business_id, period DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Table 3 — wh_client_per_location_monthly (per-location-per-period)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_client_per_location_monthly (
    business_id                 INTEGER NOT NULL,
    period                      DATE    NOT NULL,   -- YYYY-MM-01
    location_id                 INTEGER NOT NULL,
    location_name               VARCHAR(100),

    new_clients_here            INTEGER DEFAULT 0,
    clients_homed_here          INTEGER DEFAULT 0,
    active_clients_here         INTEGER DEFAULT 0,
    revenue_here                DOUBLE PRECISION DEFAULT 0,

    rank_by_new_clients         INTEGER,
    rank_by_active_clients      INTEGER,

    etl_run_at                  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (business_id, period, location_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_client_per_loc_biz
    ON wh_client_per_location_monthly (business_id, period DESC);



-- =============================================================================
-- MARKETING DOMAIN — Sprint 6
-- =============================================================================
-- 3 tables map 1:1 to the Step 2 query specs:
--   QS1 → wh_mrk_campaign_summary
--   QS2 → wh_mrk_channel_monthly
--   QS3 → wh_mrk_promo_attribution_monthly
-- (Pre-refactor wh_campaign_performance was dropped in the 2026-05-03 cleanup.)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- wh_mrk_campaign_summary — per-campaign-per-execution KPI rollup
-- Source: QS1 (Step 2)
-- Powers: Q1, Q4, Q5–Q14, Q25, Q26, Q31, Q33
-- One row per (business_id, campaign_id, execution_date).
-- Campaigns with no executions get one phantom row where execution_date IS NULL
-- to support the is_expired_but_active workflow-health flag.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wh_mrk_campaign_summary (
    business_id             BIGINT       NOT NULL,
    campaign_id             BIGINT       NOT NULL,
    execution_date          DATE         NULL,       -- NULL = never executed
    period                  DATE         NULL,       -- first-of-month of execution_date
    campaign_name           TEXT         NOT NULL,
    campaign_status         TEXT         NOT NULL,   -- completed/pending/ready
    is_active               SMALLINT     NOT NULL DEFAULT 0,
    is_recurring            SMALLINT     NOT NULL DEFAULT 0,
    channel                 TEXT         NOT NULL,   -- email/mobile/sms/unknown
    channel_code            SMALLINT     NOT NULL,   -- 1=email, 2=mobile, 3=sms, 0=unknown
    template_format_name    TEXT         NULL,
    audience_size           INTEGER      NULL,
    promo_code_string       TEXT         NULL,
    campaign_start          DATE         NULL,
    campaign_expiration     DATE         NULL,
    total_sent              INTEGER      NOT NULL DEFAULT 0,
    delivered               INTEGER      NOT NULL DEFAULT 0,
    failed                  INTEGER      NOT NULL DEFAULT 0,
    opened                  INTEGER      NOT NULL DEFAULT 0,
    clicked                 INTEGER      NOT NULL DEFAULT 0,
    delivery_rate_pct       NUMERIC(5,2) NULL,
    open_rate_pct           NUMERIC(5,2) NULL,   -- structurally NULL for SMS
    click_rate_pct          NUMERIC(5,2) NULL,
    ctr_engagement_pct      NUMERIC(5,2) NULL,
    is_expired_but_active   SMALLINT     NOT NULL DEFAULT 0,
    rank_open_in_period     INTEGER      NULL,
    rank_click_in_period    INTEGER      NULL,
    rank_reach_in_period    INTEGER      NULL,
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Expression-based unique index serves as the logical PK. PostgreSQL doesn't
-- allow expressions inside a PRIMARY KEY constraint, but a unique index on
-- COALESCE(execution_date, DATE '1900-01-01') works and matches the loader's
-- ON CONFLICT clause for upserts of phantom (never-executed) rows.
CREATE UNIQUE INDEX IF NOT EXISTS uq_wh_mrk_cs_identity
    ON wh_mrk_campaign_summary (
        business_id, campaign_id, COALESCE(execution_date, DATE '1900-01-01')
    );

CREATE INDEX IF NOT EXISTS idx_wh_mrk_cs_bp
    ON wh_mrk_campaign_summary (business_id, period);

CREATE INDEX IF NOT EXISTS idx_wh_mrk_cs_bch
    ON wh_mrk_campaign_summary (business_id, channel, period);

-- Partial index: fast lookup of expired-but-active workflow issues (Q4)
CREATE INDEX IF NOT EXISTS idx_wh_mrk_cs_expired
    ON wh_mrk_campaign_summary (business_id)
    WHERE is_expired_but_active = 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- wh_mrk_channel_monthly — per-period rollup fusing 3 data streams
-- Source: QS2 (Step 2)
-- Powers: Q2, Q3, Q19–Q24, Q27, Q28, Q32, Q34
-- One row per (business_id, period).
-- Per DD2: unsubscribe_* columns store point-in-time snapshots written at
-- ETL run. Historical values for prior periods stay in earlier rows —
-- closed periods are never overwritten.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wh_mrk_channel_monthly (
    business_id                 BIGINT       NOT NULL,
    period                      DATE         NOT NULL,   -- first-of-month

    -- Volume (from tbl_smsemailcount)
    emails_sent                 INTEGER      NOT NULL DEFAULT 0,
    sms_sent                    INTEGER      NOT NULL DEFAULT 0,
    prev_emails_sent            INTEGER      NULL,
    prev_sms_sent               INTEGER      NULL,
    emails_mom_pct              NUMERIC(8,2) NULL,
    sms_mom_pct                 NUMERIC(8,2) NULL,

    -- Campaign performance aggregated by channel
    email_campaigns_run         INTEGER      NULL,
    email_open_rate_pct         NUMERIC(5,2) NULL,
    email_click_rate_pct        NUMERIC(5,2) NULL,
    sms_campaigns_run           INTEGER      NULL,
    sms_open_rate_pct           NUMERIC(5,2) NULL,   -- structurally NULL
    sms_click_rate_pct          NUMERIC(5,2) NULL,

    -- Unsubscribe snapshot at period end
    email_unsubscribed_count    INTEGER      NULL,
    sms_unsubscribed_count      INTEGER      NULL,
    total_contacts              INTEGER      NULL,
    email_contactable           INTEGER      NULL,
    sms_contactable             INTEGER      NULL,

    -- Derived (computed at ETL write from current vs prior stored period)
    email_net_unsub_delta       INTEGER      NULL,
    sms_net_unsub_delta         INTEGER      NULL,
    email_contactable_mom_pct   NUMERIC(8,2) NULL,

    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period)
);

CREATE INDEX IF NOT EXISTS idx_wh_mrk_cm_bp
    ON wh_mrk_channel_monthly (business_id, period);


-- ─────────────────────────────────────────────────────────────────────────────
-- wh_mrk_promo_attribution_monthly — campaign revenue attribution
-- Source: QS3 (Step 2)
-- Powers: Q15–Q18, Q29, Q30
-- One row per (business_id, campaign_id, period, location_id).
-- Double tenant bind (DD4) enforced at query level — see QS3 SQL.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS wh_mrk_promo_attribution_monthly (
    business_id                 BIGINT        NOT NULL,
    campaign_id                 BIGINT        NOT NULL,
    period                      DATE          NOT NULL,   -- first-of-month
    location_id                 BIGINT        NOT NULL,
    campaign_name               TEXT          NOT NULL,
    promo_code_string           TEXT          NOT NULL,
    audience_size               INTEGER       NULL,
    redemptions                 INTEGER       NOT NULL DEFAULT 0,
    attributed_revenue          NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_discount_given        NUMERIC(18,2) NOT NULL DEFAULT 0,
    net_revenue_after_discount  NUMERIC(18,2) NOT NULL DEFAULT 0,
    revenue_per_send            NUMERIC(18,4) NULL,
    conversion_rate_pct         NUMERIC(8,4)  NULL,
    rank_in_period              INTEGER       NULL,
    rank_in_location_period     INTEGER       NULL,
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, campaign_id, period, location_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_mrk_pa_bp
    ON wh_mrk_promo_attribution_monthly (business_id, period);

CREATE INDEX IF NOT EXISTS idx_wh_mrk_pa_bl
    ON wh_mrk_promo_attribution_monthly (business_id, period, location_id);



-- =============================================================================
-- EXPENSES DOMAIN — Sprint 7
-- =============================================================================
-- 7 tables. (Pre-refactor wh_expense_summary was dropped in the 2026-05-03 cleanup.)
-- Matches: etl/transforms/expenses_etl.py upserts
-- Source of truth: Step 2 query spec v2 + Step 3 API spec v2
-- =============================================================================

-- ── 1. wh_exp_monthly_summary ────────────────────────────────────────────────
-- One row per (business_id × period). Feeds EP1 / Query 1.
-- NULL-tolerant on quarter and MoM fields per API spec.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_monthly_summary (
    business_id              INTEGER      NOT NULL,
    period                   DATE         NOT NULL,         -- first-of-month

    total_expenses           NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count        INTEGER       NOT NULL DEFAULT 0,
    avg_transaction          NUMERIC(14,2) NOT NULL DEFAULT 0,
    min_transaction          NUMERIC(14,2) NOT NULL DEFAULT 0,
    max_transaction          NUMERIC(14,2) NOT NULL DEFAULT 0,

    prev_month_expenses      NUMERIC(14,2),                 -- NULL for first month
    mom_change_pct           NUMERIC(8,2),                  -- NULL for first month
    mom_direction            TEXT,                          -- 'up'|'down'|'flat'|NULL

    ytd_total                NUMERIC(14,2) NOT NULL DEFAULT 0,   -- calendar-year YTD
    window_cumulative        NUMERIC(14,2) NOT NULL DEFAULT 0,   -- running sum over window

    current_quarter_total    NUMERIC(14,2),                 -- NULL if incomplete quarter
    prev_quarter_total       NUMERIC(14,2),                 -- NULL if insufficient data
    qoq_change_pct           NUMERIC(8,2),                  -- NULL if either quarter incomplete

    expense_rank_in_window   INTEGER      NOT NULL DEFAULT 0, -- 1 = highest-spend month
    avg_monthly_in_window    NUMERIC(14,2) NOT NULL DEFAULT 0,
    months_in_window         INTEGER      NOT NULL DEFAULT 0,

    large_txn_count          INTEGER      NOT NULL DEFAULT 0, -- txns > $100K (outlier flag)
    huge_txn_count           INTEGER      NOT NULL DEFAULT 0, -- txns > $1M (entry error flag)

    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_monthly_summary_period
    ON wh_exp_monthly_summary (period);

CREATE INDEX IF NOT EXISTS idx_wh_exp_monthly_summary_business
    ON wh_exp_monthly_summary (business_id, period DESC);


-- ── 2. wh_exp_category_breakdown ─────────────────────────────────────────────
-- One row per (business_id × period × category). Feeds EP2 / Query 2.
-- Dormant categories are ABSENT from rows — doc layer derives them.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_category_breakdown (
    business_id                INTEGER      NOT NULL,
    period                     DATE         NOT NULL,
    category_id                INTEGER      NOT NULL,
    category_name              TEXT         NOT NULL DEFAULT 'Uncategorized',

    category_total             NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count          INTEGER       NOT NULL DEFAULT 0,
    month_total                NUMERIC(14,2) NOT NULL DEFAULT 0,
    pct_of_month               NUMERIC(8,2)  NOT NULL DEFAULT 0,
    rank_in_month              INTEGER       NOT NULL DEFAULT 0,

    prev_month_total           NUMERIC(14,2),                 -- NULL if no prior month
    mom_change_pct             NUMERIC(8,2),

    baseline_3mo_avg           NUMERIC(14,2),                 -- NULL if < 2 months of prior history
    baseline_months_available  INTEGER      NOT NULL DEFAULT 0,
    pct_vs_baseline            NUMERIC(8,2),                  -- NULL when baseline insufficient
    anomaly_flag               TEXT,                          -- 'spike'|'elevated'|'normal'|'low'|'unusual_low'|NULL

    updated_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, category_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_category_bd_period
    ON wh_exp_category_breakdown (period);

CREATE INDEX IF NOT EXISTS idx_wh_exp_category_bd_business
    ON wh_exp_category_breakdown (business_id, period DESC);

-- Supports Q24-style "spending more than usual" queries directly from SQL
-- if ever needed (the AI layer primarily uses anomaly_flag in RAG chunks).
CREATE INDEX IF NOT EXISTS idx_wh_exp_category_bd_anomaly
    ON wh_exp_category_breakdown (business_id, anomaly_flag, period DESC)
    WHERE anomaly_flag IS NOT NULL;


-- ── 3. wh_exp_subcategory_breakdown ──────────────────────────────────────────
-- One row per (business_id × period × category × subcategory).
-- Only populated when the ETL fetches with include_subcategories=True.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_subcategory_breakdown (
    business_id         INTEGER      NOT NULL,
    period              DATE         NOT NULL,
    category_id         INTEGER      NOT NULL,
    category_name       TEXT         NOT NULL DEFAULT 'Uncategorized',
    subcategory_id      INTEGER      NOT NULL,
    subcategory_name    TEXT         NOT NULL DEFAULT 'Unspecified',

    subcategory_total   NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count   INTEGER       NOT NULL DEFAULT 0,
    rank_in_category    INTEGER       NOT NULL DEFAULT 0,

    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, category_id, subcategory_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_subcategory_bd_business
    ON wh_exp_subcategory_breakdown (business_id, period DESC, category_id);


-- ── 4. wh_exp_location_breakdown ─────────────────────────────────────────────
-- One row per (business_id × period × location). Per-location only; rollup
-- lives in wh_exp_monthly_summary (rollup-vs-per-location retrieval hygiene).
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_location_breakdown (
    business_id         INTEGER      NOT NULL,
    period              DATE         NOT NULL,
    location_id         INTEGER      NOT NULL,
    location_name       TEXT         NOT NULL DEFAULT 'Unknown',

    location_total      NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count   INTEGER       NOT NULL DEFAULT 0,
    month_total         NUMERIC(14,2) NOT NULL DEFAULT 0,
    pct_of_month        NUMERIC(8,2)  NOT NULL DEFAULT 0,
    rank_in_month       INTEGER       NOT NULL DEFAULT 0,

    prev_month_total    NUMERIC(14,2),
    mom_change_pct      NUMERIC(8,2),

    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, location_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_location_bd_business
    ON wh_exp_location_breakdown (business_id, period DESC);


-- ── 5. wh_exp_payment_type_breakdown ─────────────────────────────────────────
-- One row per (business_id × period × payment_type_code).
-- PaymentType enum confirmed 2026-04-21: 1=Cash, 2=Check, 3=Card.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_payment_type_breakdown (
    business_id           INTEGER      NOT NULL,
    period                DATE         NOT NULL,
    payment_type_code     INTEGER      NOT NULL,
    payment_type_label    TEXT         NOT NULL,            -- 'Cash'|'Check'|'Card'|'Type N'

    type_total            NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count     INTEGER       NOT NULL DEFAULT 0,
    month_total           NUMERIC(14,2) NOT NULL DEFAULT 0,
    pct_of_month          NUMERIC(8,2)  NOT NULL DEFAULT 0,

    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, payment_type_code)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_payment_bd_business
    ON wh_exp_payment_type_breakdown (business_id, period DESC);


-- ── 6. wh_exp_staff_attribution ──────────────────────────────────────────────
-- One row per (business_id × period × employee). PII-hardened: k-anonymity
-- (>=3 entries) is enforced at the query level, so rows with <3 entries
-- NEVER appear here. total_amount_logged is stored but NOT embedded in
-- RAG chunks by the doc generator.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_staff_attribution (
    business_id          INTEGER      NOT NULL,
    period               DATE         NOT NULL,
    employee_id          INTEGER      NOT NULL,
    employee_name        TEXT         NOT NULL DEFAULT 'Unknown',

    entries_logged       INTEGER       NOT NULL DEFAULT 0,  -- always >= 3 (k-anonymity)
    total_amount_logged  NUMERIC(14,2) NOT NULL DEFAULT 0,  -- NOT embedded in AI docs
    rank_in_month        INTEGER       NOT NULL DEFAULT 0,

    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, employee_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_staff_attr_business
    ON wh_exp_staff_attribution (business_id, period DESC);


-- ── 7. wh_exp_category_location_cross ────────────────────────────────────────
-- One row per (business_id × period × location × category). Heaviest table
-- by volume — can reach ~450 rows/tenant for multi-location, many-category,
-- 6-month windows.
-- =============================================================================
CREATE TABLE IF NOT EXISTS wh_exp_category_location_cross (
    business_id             INTEGER      NOT NULL,
    period                  DATE         NOT NULL,
    location_id             INTEGER      NOT NULL,
    location_name           TEXT         NOT NULL DEFAULT 'Unknown',
    category_id             INTEGER      NOT NULL,
    category_name           TEXT         NOT NULL DEFAULT 'Uncategorized',

    cross_total             NUMERIC(14,2) NOT NULL DEFAULT 0,
    transaction_count       INTEGER       NOT NULL DEFAULT 0,
    pct_of_location_month   NUMERIC(8,2)  NOT NULL DEFAULT 0,
    rank_in_location_month  INTEGER       NOT NULL DEFAULT 0,

    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, period, location_id, category_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_exp_cat_loc_cross_business
    ON wh_exp_category_location_cross (business_id, period DESC, location_id);



-- =============================================================================
-- PROMOS DOMAIN — Sprint 8
-- =============================================================================
-- 5 tables for promo redemption analytics, read by doc_generator.promos.py.
--
-- Conventions:
--   • business_id column on every table (tenant isolation enforced at write+read)
--   • period_start as DATE column, NULLABLE for catalog-style rows (Lesson 3)
--   • All currency as NUMERIC(20, 2) — matches API spec types
--   • Composite indexes on (business_id, period_start) for query performance
--   • generated_at TIMESTAMP for staleness checks
-- =============================================================================


-- ── 1. wh_promo_monthly ───────────────────────────────────────────────────────
-- Powers: promo_monthly_summary chunks (rollup, location_id=0)
-- Source: EP1 /api/v1/leo/promos/monthly

CREATE TABLE IF NOT EXISTS wh_promo_monthly (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    period_start                    DATE            NOT NULL,
    total_visits                    INTEGER         NOT NULL DEFAULT 0,
    promo_redemptions               INTEGER         NOT NULL DEFAULT 0,
    distinct_codes_used             INTEGER         NOT NULL DEFAULT 0,
    promo_visit_pct                 NUMERIC(5, 2)   NOT NULL DEFAULT 0,
    total_discount_given            NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    avg_discount_per_redemption     NUMERIC(10, 2),
    prev_month_redemptions          INTEGER,
    prev_month_discount             NUMERIC(20, 2),
    generated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_promo_monthly_biz_period
    ON wh_promo_monthly (business_id, period_start DESC);


-- ── 2. wh_promo_codes ─────────────────────────────────────────────────────────
-- Powers: promo_code_monthly + promo_code_window_total chunks
-- Source: EP2 /api/v1/leo/promos/codes (both granularities)
--
-- period_start IS NULL → window-total row (Lesson 3 — catalog-style with NULL period)
-- period_start IS NOT NULL → monthly per-code row
-- promo_code_string + promo_label NULLABLE for orphan handling (Lesson per N1)

CREATE TABLE IF NOT EXISTS wh_promo_codes (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    period_start                    DATE,           -- NULL for window-total rows
    promo_id                        INTEGER         NOT NULL,
    promo_code_string               VARCHAR(20),    -- NULL for orphans
    promo_label                     VARCHAR(150),   -- NULL when Desc empty
    promo_amount_metadata           NUMERIC(20, 2), -- NULL for orphans
    is_active                       SMALLINT,
    expiration_date                 DATE,
    redemptions                     INTEGER         NOT NULL DEFAULT 0,
    total_discount                  NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    avg_discount                    NUMERIC(10, 2),
    max_single_discount             NUMERIC(10, 2),
    is_expired_now                  SMALLINT,       -- only set on window-total rows
    generated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Composite uniqueness allows both monthly + window-total rows for same code
    -- by treating NULL period as distinct via COALESCE
    UNIQUE (business_id, promo_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_promo_codes_biz_period
    ON wh_promo_codes (business_id, period_start DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_wh_promo_codes_biz_promo
    ON wh_promo_codes (business_id, promo_id);


-- ── 3. wh_promo_locations ─────────────────────────────────────────────────────
-- Powers: promo_location_rollup chunks (per-location aggregate)
-- Source: EP3 /api/v1/leo/promos/locations?shape=rollup
--
-- The by_code shape feeds wh_promo_location_codes (separate table below).

CREATE TABLE IF NOT EXISTS wh_promo_locations (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    period_start                    DATE            NOT NULL,
    location_id                     INTEGER         NOT NULL,
    location_name                   VARCHAR(255),   -- NULL allowed if location deleted
    total_promo_redemptions         INTEGER         NOT NULL DEFAULT 0,
    distinct_codes_used             INTEGER         NOT NULL DEFAULT 0,
    total_discount_given            NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    avg_discount_per_redemption     NUMERIC(10, 2),
    generated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_id, period_start, location_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_promo_locations_biz_period
    ON wh_promo_locations (business_id, period_start DESC);


-- ── 4. wh_promo_location_codes ────────────────────────────────────────────────
-- Powers: promo_location_monthly chunks (per-code per-location detail)
-- Source: EP3 /api/v1/leo/promos/locations?shape=by_code

CREATE TABLE IF NOT EXISTS wh_promo_location_codes (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    period_start                    DATE            NOT NULL,
    location_id                     INTEGER         NOT NULL,
    location_name                   VARCHAR(255),
    promo_id                        INTEGER         NOT NULL,
    promo_code_string               VARCHAR(20),    -- NULL for orphans
    promo_label                     VARCHAR(150),
    redemptions                     INTEGER         NOT NULL DEFAULT 0,
    total_discount                  NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    avg_discount                    NUMERIC(10, 2),
    generated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_id, period_start, location_id, promo_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_promo_location_codes_biz_period
    ON wh_promo_location_codes (business_id, period_start DESC);


-- ── 5. wh_promo_catalog_health ────────────────────────────────────────────────
-- Powers: promo_catalog_health chunks (catalog state, period=NULL)
-- Source: EP4 /api/v1/leo/promos/catalog-health
--
-- This is a snapshot table — overwrite-on-write semantics.
-- ETL DELETEs rows for the business_id then INSERTs the fresh snapshot.

CREATE TABLE IF NOT EXISTS wh_promo_catalog_health (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    promo_id                        INTEGER         NOT NULL,
    promo_code_string               VARCHAR(20),
    promo_label                     VARCHAR(150),
    is_active                       SMALLINT,
    expiration_date                 DATE,
    is_expired                      SMALLINT        NOT NULL DEFAULT 0,
    active_but_expired              SMALLINT        NOT NULL DEFAULT 0,
    redemptions_last_90d            INTEGER         NOT NULL DEFAULT 0,
    is_dormant                      SMALLINT        NOT NULL DEFAULT 0,
    snapshot_date                   DATE            NOT NULL,
    generated_at                    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (business_id, promo_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_promo_catalog_health_biz
    ON wh_promo_catalog_health (business_id);



-- =============================================================================
-- GIFT CARDS DOMAIN — Sprint 9
-- =============================================================================
-- 8 tables, one per analytics endpoint (EP1–EP8 from API spec v1.0).
-- All upserts use INSERT ... ON CONFLICT DO UPDATE inside a transaction
-- (idempotent — Lesson 17 from prior sprints).
--
-- Snapshot tables use (business_id, snapshot_date) as UNIQUE — re-running the
-- ETL on the same snapshot_date overwrites; running on a new snapshot_date
-- appends a new historical row.
--
-- Per-period tables use (business_id, period_start) — same upsert behavior.
-- Bucketed tables (aging, denomination) include the bucket label in UNIQUE.
--
-- All money: NUMERIC(20,6) — matches tbl_giftcard.GiftCardBalance precision.
-- All percentages: NUMERIC(7,2) — allows up to 99999.99 for huge MoM/YoY spikes.
-- =============================================================================


-- ── EP1: wh_giftcard_monthly ─────────────────────────────────────────────────
-- Per-month redemption + activation summary. One row per (business_id, period).
-- Months with zero redemption AND zero activation are not emitted (per spec).

CREATE TABLE IF NOT EXISTS wh_giftcard_monthly (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    period_start                    DATE            NOT NULL,
    redemption_count                INTEGER         NOT NULL DEFAULT 0,
    redemption_amount_total         NUMERIC(20,6)   NOT NULL DEFAULT 0,
    distinct_cards_redeemed         INTEGER         NOT NULL DEFAULT 0,
    activation_count                INTEGER         NOT NULL DEFAULT 0,
    weekend_redemption_count        INTEGER         NOT NULL DEFAULT 0,
    weekday_redemption_count        INTEGER         NOT NULL DEFAULT 0,
    avg_uplift_per_visit            NUMERIC(20,6)   NOT NULL DEFAULT 0,
    uplift_total                    NUMERIC(20,6)   NOT NULL DEFAULT 0,
    mom_redemption_pct              NUMERIC(7,2),
    mom_activation_pct              NUMERIC(7,2),
    yoy_redemption_pct              NUMERIC(7,2),
    generated_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_monthly_biz_period
    ON wh_giftcard_monthly (business_id, period_start);


-- ── EP2: wh_giftcard_liability_snapshot ──────────────────────────────────────
-- Outstanding liability snapshot. One row per (business_id, snapshot_date).
-- For Q6 trend ("liability over trailing 6 months") the ETL writes one row
-- per month-end snapshot_date; multiple historical rows accumulate over time.

CREATE TABLE IF NOT EXISTS wh_giftcard_liability_snapshot (
    id                                      SERIAL          PRIMARY KEY,
    business_id                             INTEGER         NOT NULL,
    snapshot_date                           DATE            NOT NULL,
    active_card_count                       INTEGER         NOT NULL DEFAULT 0,
    outstanding_liability_total             NUMERIC(20,6)   NOT NULL DEFAULT 0,
    avg_remaining_balance_excl_drained      NUMERIC(20,6)   NOT NULL DEFAULT 0,
    avg_remaining_balance_incl_drained      NUMERIC(20,6)   NOT NULL DEFAULT 0,
    drained_active_count                    INTEGER         NOT NULL DEFAULT 0,
    median_remaining_balance                NUMERIC(20,6)   NOT NULL DEFAULT 0,
    generated_at                            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_liability_biz_date
    ON wh_giftcard_liability_snapshot (business_id, snapshot_date DESC);


-- ── EP3: wh_giftcard_by_staff ────────────────────────────────────────────────
-- Per-staff per-month redemption breakdown. UNIQUE (biz, staff, period).

CREATE TABLE IF NOT EXISTS wh_giftcard_by_staff (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    staff_id                        INTEGER         NOT NULL,
    staff_name                      VARCHAR(150)    NOT NULL,
    is_active                       SMALLINT        NOT NULL DEFAULT 1,
    period_start                    DATE            NOT NULL,
    redemption_count                INTEGER         NOT NULL DEFAULT 0,
    redemption_amount_total         NUMERIC(20,6)   NOT NULL DEFAULT 0,
    distinct_cards_redeemed         INTEGER         NOT NULL DEFAULT 0,
    rank_in_period                  INTEGER         NOT NULL DEFAULT 0,
    generated_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, staff_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_by_staff_biz_period
    ON wh_giftcard_by_staff (business_id, period_start);


-- ── EP4: wh_giftcard_by_location ─────────────────────────────────────────────
-- Per-location per-month redemption with within-org share + MoM.
-- UNIQUE (biz, location, period).

CREATE TABLE IF NOT EXISTS wh_giftcard_by_location (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    location_id                     INTEGER         NOT NULL,
    location_name                   VARCHAR(150)    NOT NULL,
    period_start                    DATE            NOT NULL,
    redemption_count                INTEGER         NOT NULL DEFAULT 0,
    redemption_amount_total         NUMERIC(20,6)   NOT NULL DEFAULT 0,
    distinct_cards_redeemed         INTEGER         NOT NULL DEFAULT 0,
    pct_of_org_redemption           NUMERIC(7,2),
    mom_redemption_pct              NUMERIC(7,2),
    generated_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_by_location_biz_period
    ON wh_giftcard_by_location (business_id, period_start);


-- ── EP5: wh_giftcard_aging_snapshot ──────────────────────────────────────────
-- 5 rows per (biz, snapshot_date): 4 aging_bucket rows + 1 dormancy_summary row.
-- age_bucket label: "0-30" | "31-90" | "91-180" | "181+" | "all" (summary).
-- row_type: "aging_bucket" | "dormancy_summary"

CREATE TABLE IF NOT EXISTS wh_giftcard_aging_snapshot (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    snapshot_date                   DATE            NOT NULL,
    row_type                        VARCHAR(30)     NOT NULL,
    age_bucket                      VARCHAR(20)     NOT NULL,
    card_count                      INTEGER         NOT NULL DEFAULT 0,
    liability_amount                NUMERIC(20,6)   NOT NULL DEFAULT 0,
    pct_of_total_liability          NUMERIC(7,2),
    never_redeemed_in_bucket        INTEGER         NOT NULL DEFAULT 0,
    avg_days_to_first_redemption    NUMERIC(8,1),
    longest_dormant_card_id         INTEGER,
    longest_dormant_days            INTEGER,
    generated_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, snapshot_date, age_bucket)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_aging_biz_date
    ON wh_giftcard_aging_snapshot (business_id, snapshot_date DESC);


-- ── EP6: wh_giftcard_anomalies_snapshot ──────────────────────────────────────
-- ALWAYS-EMIT — one row per (biz, snapshot_date) even when all counts are zero
-- (Q31 acceptance). drained_active_card_ids stored as INTEGER[].
-- period_start/end define the window for refunded_redemption_* counts.

CREATE TABLE IF NOT EXISTS wh_giftcard_anomalies_snapshot (
    id                                      SERIAL          PRIMARY KEY,
    business_id                             INTEGER         NOT NULL,
    snapshot_date                           DATE            NOT NULL,
    drained_active_count                    INTEGER         NOT NULL DEFAULT 0,
    drained_active_card_ids                 INTEGER[]       NOT NULL DEFAULT '{}',
    deactivated_count                       INTEGER         NOT NULL DEFAULT 0,
    deactivated_value_total_derived         NUMERIC(20,6)   NOT NULL DEFAULT 0,
    refunded_redemption_count               INTEGER         NOT NULL DEFAULT 0,
    refunded_redemption_amount              NUMERIC(20,6)   NOT NULL DEFAULT 0,
    period_start                            DATE,
    period_end                              DATE,
    generated_at                            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_anomalies_biz_date
    ON wh_giftcard_anomalies_snapshot (business_id, snapshot_date DESC);


-- ── EP7: wh_giftcard_denomination_snapshot ───────────────────────────────────
-- 6 rows per (biz, snapshot_date) — one per denomination bucket.
-- Buckets: "$25 or less" | "$26-$50" | "$51-$100" | "$101-$200"
--        | "$201-$500"   | "$500+"

CREATE TABLE IF NOT EXISTS wh_giftcard_denomination_snapshot (
    id                              SERIAL          PRIMARY KEY,
    business_id                     INTEGER         NOT NULL,
    snapshot_date                   DATE            NOT NULL,
    denomination_bucket             VARCHAR(20)     NOT NULL,
    card_count                      INTEGER         NOT NULL DEFAULT 0,
    total_value_issued              NUMERIC(20,6)   NOT NULL DEFAULT 0,
    avg_face_value                  NUMERIC(20,6)   NOT NULL DEFAULT 0,
    pct_of_cards                    NUMERIC(7,2)    NOT NULL DEFAULT 0,
    generated_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, snapshot_date, denomination_bucket)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_denom_biz_date
    ON wh_giftcard_denomination_snapshot (business_id, snapshot_date DESC);


-- ── EP8: wh_giftcard_health_snapshot ─────────────────────────────────────────
-- Card population health metrics. One row per (biz, snapshot_date).

CREATE TABLE IF NOT EXISTS wh_giftcard_health_snapshot (
    id                                          SERIAL          PRIMARY KEY,
    business_id                                 INTEGER         NOT NULL,
    snapshot_date                               DATE            NOT NULL,
    total_cards_issued                          INTEGER         NOT NULL DEFAULT 0,
    cards_with_redemption                       INTEGER         NOT NULL DEFAULT 0,
    redemption_rate_pct                         NUMERIC(7,2),
    single_visit_drained_count                  INTEGER         NOT NULL DEFAULT 0,
    multi_visit_redeemed_count                  INTEGER         NOT NULL DEFAULT 0,
    single_visit_drained_pct_of_redeemed        NUMERIC(7,2),
    multi_visit_redeemed_pct_of_redeemed        NUMERIC(7,2),
    distinct_customer_redeemers                 INTEGER         NOT NULL DEFAULT 0,
    generated_at                                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at                                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (business_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_giftcard_health_biz_date
    ON wh_giftcard_health_snapshot (business_id, snapshot_date DESC);



-- =============================================================================
-- FORMS DOMAIN — Sprint 10
-- =============================================================================
-- 4 tables, all composite-PK on (business_id, snapshot_date) or
-- (business_id, period_start). All idempotent via INSERT ... ON CONFLICT.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- FQ1 · Catalog snapshot — 1 row per (biz, snapshot_date)
-- Powers: F1, F3, F8, F11
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_form_catalog_snapshot (
    business_id                  INT          NOT NULL,
    snapshot_date                DATE         NOT NULL,
    total_template_count         INT          NOT NULL DEFAULT 0,
    active_template_count        INT          NOT NULL DEFAULT 0,
    inactive_template_count      INT          NOT NULL DEFAULT 0,
    active_dormant_count         INT          NOT NULL DEFAULT 0,
    inactive_dormant_count       INT          NOT NULL DEFAULT 0,
    lifetime_submission_total    INT          NOT NULL DEFAULT 0,
    recent_90d_submission_total  INT          NOT NULL DEFAULT 0,
    most_recent_template_added   TIMESTAMP,
    distinct_category_ids        INTEGER[]    NOT NULL DEFAULT '{}',
    updated_at                   TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_form_catalog_biz
    ON wh_form_catalog_snapshot (business_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- FQ2 · Monthly summary — 1 row per (biz, period_start)
-- Powers: F2, F4, F5, F6, F12, S1
-- Months with zero activity are NOT emitted (R7).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_form_monthly (
    business_id                  INT          NOT NULL,
    period_start                 DATE         NOT NULL,
    submission_count             INT          NOT NULL DEFAULT 0,
    ready_count                  INT          NOT NULL DEFAULT 0,
    complete_count               INT          NOT NULL DEFAULT 0,
    approved_count               INT          NOT NULL DEFAULT 0,
    distinct_forms_used          INT          NOT NULL DEFAULT 0,
    distinct_customers_filling   INT          NOT NULL DEFAULT 0,
    mom_submission_pct           NUMERIC(8,2),
    yoy_submission_pct           NUMERIC(8,2),
    updated_at                   TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_form_monthly_biz_period
    ON wh_form_monthly (business_id, period_start DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- FQ3 · Per-form snapshot — 1 row per (biz, form_id, snapshot_date)
-- Powers: F7, F8, F11
-- Includes inactive templates so F8/F11 surface them with is_active flag.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_form_per_form_snapshot (
    business_id                  INT          NOT NULL,
    snapshot_date                DATE         NOT NULL,
    form_id                      INT          NOT NULL,
    form_name                    VARCHAR(50)  NOT NULL,
    form_description             VARCHAR(500),
    is_active                    BOOLEAN      NOT NULL,
    category_id                  INT          NOT NULL,
    template_created_at          TIMESTAMP    NOT NULL,
    lifetime_submission_count    INT          NOT NULL DEFAULT 0,
    complete_count               INT          NOT NULL DEFAULT 0,
    approved_count               INT          NOT NULL DEFAULT 0,
    ready_count                  INT          NOT NULL DEFAULT 0,
    submissions_last_30d         INT          NOT NULL DEFAULT 0,
    submissions_last_90d         INT          NOT NULL DEFAULT 0,
    most_recent_submission_at    TIMESTAMP,
    distinct_customers           INT          NOT NULL DEFAULT 0,
    is_dormant                   BOOLEAN      NOT NULL,
    is_active_dormant            BOOLEAN      NOT NULL,
    completion_rate_pct          NUMERIC(5,2),
    rank_by_submissions          INT          NOT NULL,
    updated_at                   TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_id, snapshot_date, form_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_form_per_form_biz_snap
    ON wh_form_per_form_snapshot (business_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_wh_form_per_form_dormant
    ON wh_form_per_form_snapshot (business_id, is_active_dormant)
    WHERE is_active_dormant = TRUE;


-- ─────────────────────────────────────────────────────────────────────────────
-- FQ4 · Lifecycle snapshot — 1 row per (biz, snapshot_date)
-- Powers: F9, F10, F13
-- ⚠️ ALWAYS-EMIT contract — must always have one row per snapshot, even when
-- counts are all zero. Mirrors gift cards G6 pattern.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_form_lifecycle_snapshot (
    business_id                  INT          NOT NULL,
    snapshot_date                DATE         NOT NULL,
    total_submissions            INT          NOT NULL DEFAULT 0,
    ready_count                  INT          NOT NULL DEFAULT 0,
    complete_count               INT          NOT NULL DEFAULT 0,
    approved_count               INT          NOT NULL DEFAULT 0,
    unknown_status_count         INT          NOT NULL DEFAULT 0,
    completion_rate_pct          NUMERIC(5,2),
    stuck_ready_count            INT          NOT NULL DEFAULT 0,
    stuck_ready_total_age_days   INT          NOT NULL DEFAULT 0,
    most_recent_submission_at    TIMESTAMP,
    stuck_ready_submission_ids   INTEGER[]    NOT NULL DEFAULT '{}',
    updated_at                   TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_form_lifecycle_biz
    ON wh_form_lifecycle_snapshot (business_id);



-- =============================================================================
-- MEMBERSHIPS DOMAIN — Sprint 11
-- =============================================================================
-- 2 tables mirroring the two API endpoints:
--   wh_membership_units    (Set A — unit grain)
--   wh_membership_monthly  (Set B — location-month grain)
--
-- Both keyed on business_id for tenant isolation.
-- (Pre-refactor wh_subscription_revenue was dropped in the 2026-05-03 cleanup.)
-- =============================================================================


-- ─── Set A: Unit-grain memberships snapshot ─────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_membership_units (
    -- Tenant + snapshot
    business_id                  INT          NOT NULL,
    as_of_date                   DATE         NOT NULL,

    -- Subscription identity
    subscription_id              INT          NOT NULL,
    location_id                  INT          NOT NULL,
    customer_id                  INT          NOT NULL,
    customer_name                TEXT,
    service_id                   INT          NOT NULL,
    service_name                 TEXT,

    -- Pricing
    amount                       DECIMAL(10,2) NOT NULL,
    discount                     DECIMAL(10,2) NOT NULL DEFAULT 0,
    net_amount                   DECIMAL(10,2) NOT NULL,
    interval_days                INT          NOT NULL,
    interval_bucket              TEXT         NOT NULL,
    monthly_equivalent_revenue   DECIMAL(10,2) NOT NULL,
    estimated_ltv                DECIMAL(10,2) NOT NULL DEFAULT 0,

    -- Lifecycle
    created_at                   TIMESTAMPTZ  NOT NULL,
    canceled_at                  TIMESTAMPTZ,
    is_active                    SMALLINT     NOT NULL,
    is_reactivation              SMALLINT     NOT NULL DEFAULT 0,
    tenure_days                  INT          NOT NULL,

    -- Billing
    next_execution_date          TIMESTAMPTZ,
    days_until_next_charge       INT,
    is_due_in_7_days             SMALLINT     NOT NULL DEFAULT 0,
    total_charge_count           INT          NOT NULL DEFAULT 0,
    approved_charge_count        INT          NOT NULL DEFAULT 0,
    failed_charge_count          INT          NOT NULL DEFAULT 0,
    total_billed                 DECIMAL(10,2) NOT NULL DEFAULT 0,
    last_successful_charge_at    TIMESTAMPTZ,
    days_since_last_charge       INT,

    -- Usage (G2 heuristic)
    visit_count_in_window        INT          NOT NULL DEFAULT 0,
    last_visit_at                TIMESTAMPTZ,
    is_used                      SMALLINT     NOT NULL DEFAULT 0,

    -- Bookkeeping
    etl_run_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, as_of_date, subscription_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_mem_units_biz_active
    ON wh_membership_units (business_id, is_active);
CREATE INDEX IF NOT EXISTS idx_wh_mem_units_biz_loc
    ON wh_membership_units (business_id, location_id);
CREATE INDEX IF NOT EXISTS idx_wh_mem_units_biz_service
    ON wh_membership_units (business_id, service_id);
CREATE INDEX IF NOT EXISTS idx_wh_mem_units_biz_due
    ON wh_membership_units (business_id, is_due_in_7_days)
    WHERE is_due_in_7_days = 1;


-- ─── Set B: Monthly summary per location ───────────────────────────────────
CREATE TABLE IF NOT EXISTS wh_membership_monthly (
    business_id          INT          NOT NULL,
    location_id          INT          NOT NULL,
    month_start          DATE         NOT NULL,

    -- Flow metrics
    new_signups          INT          NOT NULL DEFAULT 0,
    reactivations        INT          NOT NULL DEFAULT 0,
    cancellations        INT          NOT NULL DEFAULT 0,

    -- Stock metrics
    active_at_month_end  INT          NOT NULL DEFAULT 0,
    mrr                  DECIMAL(10,2) NOT NULL DEFAULT 0,
    avg_discount         DECIMAL(10,2),

    -- Billing for the month
    gross_billed         DECIMAL(10,2) NOT NULL DEFAULT 0,
    approved_charges     INT          NOT NULL DEFAULT 0,
    failed_charges       INT          NOT NULL DEFAULT 0,

    -- Trend (LAG-derived by backend)
    prev_mrr             DECIMAL(10,2),
    mrr_mom_pct          DECIMAL(8,2),
    prev_active          INT,
    churn_rate_pct       DECIMAL(8,2),

    etl_run_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (business_id, location_id, month_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_mem_monthly_biz_month
    ON wh_membership_monthly (business_id, month_start);


-- =============================================================================
-- End of warehouse_schema.sql — 48 tables total
--   Revenue: wh_monthly_revenue, wh_daily_revenue, wh_payment_breakdown        (3)
--   Appointments: wh_appt_*                                                    (4)
--   Staff: wh_staff_performance_monthly, wh_staff_summary, wh_staff_attendance (3)
--   Services: wh_svc_*                                                         (5)
--   Clients: wh_client_*                                                       (3)
--   Marketing: wh_mrk_*                                                        (3)
--   Expenses: wh_exp_*                                                         (7)
--   Promos: wh_promo_*                                                         (5)
--   Gift Cards: wh_giftcard_*                                                  (8)
--   Forms: wh_form_*                                                           (4)
--   Memberships: wh_membership_*                                               (2)
--   Meta: wh_etl_log                                                           (1)
-- =============================================================================