-- =============================================================================
-- Analytics warehouse schema (PostgreSQL)
-- =============================================================================
-- Denormalized tables populated by ETL from production MySQL. Read by
-- warehouse_client, document generator, and downstream RAG/pgvector flows.
-- No foreign keys between warehouse tables. Use UNIQUE constraints with
-- ON CONFLICT ... DO UPDATE in ETL for idempotent upserts.
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
-- wh_staff_performance — Monthly staff KPIs (visits, revenue, commission, ratings).
-- Source: tbl_visit, tbl_service_visit, tbl_emp_reviews, tbl_calendarevent, tbl_emp.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_staff_performance (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    employee_name VARCHAR(150) NOT NULL DEFAULT '',
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_visits INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_tips DECIMAL(15, 2) NOT NULL DEFAULT 0,
    total_commission DECIMAL(15, 2) NOT NULL DEFAULT 0,
    appointments_booked INTEGER NOT NULL DEFAULT 0,
    appointments_completed INTEGER NOT NULL DEFAULT 0,
    appointments_cancelled INTEGER NOT NULL DEFAULT 0,
    avg_rating DECIMAL(3, 2),
    review_count INTEGER NOT NULL DEFAULT 0,
    utilisation_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_staff_performance_dim UNIQUE (business_id, employee_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_staff_performance_business_id ON wh_staff_performance (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_staff_performance_business_period ON wh_staff_performance (business_id, period_start);
CREATE INDEX IF NOT EXISTS idx_wh_staff_performance_business_employee ON wh_staff_performance (business_id, employee_id);

-- -----------------------------------------------------------------------------
-- wh_service_performance — Monthly service KPIs (bookings, revenue, pricing spread).
-- Source: tbl_service_visit, tbl_visit, tbl_service.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_service_performance (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    service_name VARCHAR(200) NOT NULL DEFAULT '',
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    booking_count INTEGER NOT NULL DEFAULT 0,
    revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    avg_price DECIMAL(15, 2) NOT NULL DEFAULT 0,
    min_price DECIMAL(15, 2) NOT NULL DEFAULT 0,
    max_price DECIMAL(15, 2) NOT NULL DEFAULT 0,
    unique_customers INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_service_performance_dim UNIQUE (business_id, service_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_service_performance_business_id ON wh_service_performance (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_service_performance_business_period ON wh_service_performance (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_client_metrics — Per-customer lifetime / retention metrics.
-- Source: tbl_visit, tbl_custorg.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_client_metrics (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    first_visit_date DATE,
    last_visit_date DATE,
    total_visits INTEGER NOT NULL DEFAULT 0,
    total_spend DECIMAL(15, 2) NOT NULL DEFAULT 0,
    avg_spend_per_visit DECIMAL(15, 2) NOT NULL DEFAULT 0,
    loyalty_points INTEGER NOT NULL DEFAULT 0,
    days_since_last_visit INTEGER,
    visit_frequency_days DECIMAL(8, 2),
    is_churned BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_client_metrics_dim UNIQUE (business_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_wh_client_metrics_business_id ON wh_client_metrics (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_client_metrics_business_last_visit ON wh_client_metrics (business_id, last_visit_date);
CREATE INDEX IF NOT EXISTS idx_wh_client_metrics_business_churned ON wh_client_metrics (business_id, is_churned);

-- -----------------------------------------------------------------------------
-- wh_appointment_metrics — Monthly appointment funnel and sign-in sourced counts.
-- Source: tbl_calendarevent, tbl_custsignin.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_appointment_metrics (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_booked INTEGER NOT NULL DEFAULT 0,
    confirmed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    cancelled_count INTEGER NOT NULL DEFAULT 0,
    no_show_count INTEGER NOT NULL DEFAULT 0,
    walkin_count INTEGER NOT NULL DEFAULT 0,
    app_booking_count INTEGER NOT NULL DEFAULT 0,
    cancellation_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    completion_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_appointment_metrics_dim UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_appointment_metrics_business_id ON wh_appointment_metrics (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_appointment_metrics_business_period ON wh_appointment_metrics (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_expense_summary — Monthly expenses by category (P&L / cost analysis).
-- Source: tbl_expense, tbl_expense_category, tbl_expense_subcategory.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_expense_summary (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    category_id INTEGER NOT NULL DEFAULT 0,
    category_name VARCHAR(150) NOT NULL DEFAULT '',
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_amount DECIMAL(15, 2) NOT NULL DEFAULT 0,
    expense_count INTEGER NOT NULL DEFAULT 0,
    avg_expense DECIMAL(15, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_expense_summary_dim UNIQUE (business_id, location_id, category_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_expense_summary_business_id ON wh_expense_summary (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_expense_summary_business_period ON wh_expense_summary (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_review_summary — Monthly ratings from employee, visit, and Google reviews.
-- Source: tbl_emp_reviews, tbl_visit_review, tbl_google_review (via org linkage in ETL).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_review_summary (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    emp_review_count INTEGER NOT NULL DEFAULT 0,
    emp_avg_rating DECIMAL(3, 2),
    visit_review_count INTEGER NOT NULL DEFAULT 0,
    visit_avg_rating DECIMAL(3, 2),
    google_review_count INTEGER NOT NULL DEFAULT 0,
    google_avg_rating DECIMAL(3, 2),
    google_bad_review_count INTEGER NOT NULL DEFAULT 0,
    total_review_count INTEGER NOT NULL DEFAULT 0,
    overall_avg_rating DECIMAL(3, 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_review_summary_dim UNIQUE (business_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_review_summary_business_id ON wh_review_summary (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_review_summary_business_period ON wh_review_summary (business_id, period_start);

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
-- wh_campaign_performance — Per-campaign execution metrics (email / marketing sends).
-- Source: tbl_mrkcampaign, tbl_executecampaign.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_campaign_performance (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    campaign_name VARCHAR(200) NOT NULL DEFAULT '',
    execution_date DATE NOT NULL,
    is_recurring BOOLEAN NOT NULL DEFAULT FALSE,
    total_sent INTEGER NOT NULL DEFAULT 0,
    successful_sent INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    opened_count INTEGER NOT NULL DEFAULT 0,
    clicked_count INTEGER NOT NULL DEFAULT 0,
    open_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    click_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    fail_rate DECIMAL(5, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_campaign_performance_dim UNIQUE (business_id, campaign_id, execution_date)
);

CREATE INDEX IF NOT EXISTS idx_wh_campaign_performance_business_id ON wh_campaign_performance (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_campaign_performance_business_execution ON wh_campaign_performance (business_id, execution_date);

-- -----------------------------------------------------------------------------
-- wh_attendance_summary — Monthly staff attendance (days worked, hours from parsed times).
-- Source: tbl_attendance, tbl_emp.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_attendance_summary (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    employee_name VARCHAR(150) NOT NULL DEFAULT '',
    location_id INTEGER NOT NULL DEFAULT 0,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    days_worked INTEGER NOT NULL DEFAULT 0,
    total_hours_worked DECIMAL(8, 2) NOT NULL DEFAULT 0,
    avg_hours_per_day DECIMAL(6, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_attendance_summary_dim UNIQUE (business_id, employee_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_attendance_summary_business_id ON wh_attendance_summary (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_attendance_summary_business_period ON wh_attendance_summary (business_id, period_start);

-- -----------------------------------------------------------------------------
-- wh_subscription_revenue — Monthly subscription / MRR style aggregates.
-- Source: tbl_custsubscription.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wh_subscription_revenue (
    id BIGSERIAL PRIMARY KEY,
    business_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL DEFAULT 0,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    active_subscriptions INTEGER NOT NULL DEFAULT 0,
    new_subscriptions INTEGER NOT NULL DEFAULT 0,
    cancelled_subscriptions INTEGER NOT NULL DEFAULT 0,
    gross_subscription_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    net_subscription_revenue DECIMAL(15, 2) NOT NULL DEFAULT 0,
    avg_subscription_value DECIMAL(15, 2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wh_subscription_revenue_dim UNIQUE (business_id, location_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_wh_subscription_revenue_business_id ON wh_subscription_revenue (business_id);
CREATE INDEX IF NOT EXISTS idx_wh_subscription_revenue_business_period ON wh_subscription_revenue (business_id, period_start);

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
-- APPEND TO: warehouse_schema.sql
-- =============================================================================
-- Appointments domain warehouse tables.
-- Add these after wh_appointment_metrics (the existing basic funnel table).
-- These 4 tables replace wh_appointment_metrics for RAG purposes —
-- they carry the full field set needed by the appointments doc generator.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- wh_appt_monthly_summary
-- Monthly appointment funnel per location + org rollup (location_id = 0).
-- Extends wh_appointment_metrics with time slots, duration, MoM, rollup flag.
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
-- APPEND TO: warehouse_schema.sql
-- =============================================================================
-- Staff Performance domain warehouse tables.
-- Add these after the existing wh_staff_performance table.
--
-- WHY NEW TABLES INSTEAD OF ALTERING wh_staff_performance:
--   wh_staff_performance has UNIQUE(business_id, employee_id, period_start).
--   Our Q1 grain is per (staff × location × period) — a staff member working
--   at two locations in the same month needs two rows. That would violate the
--   existing constraint. Same pattern as wh_appointment_metrics → wh_appt_*:
--   leave the existing table for backwards compat, add new tables alongside.
--
-- NEW TABLES:
--   wh_staff_performance_monthly  — Q1: KPIs per staff × location × period
--   wh_staff_summary              — Q2: all-time totals per staff
--   wh_staff_attendance           — Q4: hours worked per staff × location × period
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
-- Services Domain — Warehouse Tables
-- Append to infra/init_db.sql
-- 5 tables matching the 5 API endpoints / query sets
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
-- Services Domain — Warehouse Tables
-- Append to infra/init_db.sql
-- 5 tables matching the 5 API endpoints / query sets
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
-- Clients domain warehouse tables.
-- 3 tables support the 23 acceptance questions from the Clients sprint.
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


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification query — run after ETL to confirm population
-- ─────────────────────────────────────────────────────────────────────────────
--
-- SELECT
--     'wh_client_retention'              AS table_name,
--     COUNT(*)                           AS rows
-- FROM wh_client_retention WHERE business_id = 42
-- UNION ALL
-- SELECT 'wh_client_cohort_monthly',     COUNT(*)
-- FROM wh_client_cohort_monthly WHERE business_id = 42
-- UNION ALL
-- SELECT 'wh_client_per_location_monthly', COUNT(*)
-- FROM wh_client_per_location_monthly WHERE business_id = 42;
--
-- Expected after initial ETL run for biz 42 (based on fixtures):
--              table_name             | rows
-- ------------------------------------+------
--  wh_client_retention                |   38
--  wh_client_cohort_monthly           |    3
--  wh_client_per_location_monthly     |    2


-- =============================================================================
-- MARKETING DOMAIN — Sprint 6
-- =============================================================================
--
-- Tables map 1:1 to the Step 2 query specs:
--   QS1 → wh_mrk_campaign_summary
--   QS2 → wh_mrk_channel_monthly
--   QS3 → wh_mrk_promo_attribution_monthly
--
-- Naming convention matches prior domains (wh_appt_*, wh_svc_*, wh_client_*).
-- Primary keys + indexes follow the same (business_id, period) pattern.
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

-- End of Marketing Domain warehouse schema additions.


-- =============================================================================
-- APPEND TO: infra/warehouse_schema.sql
-- =============================================================================
-- Expenses domain warehouse tables (7 total).
--
-- Paste this block at the end of warehouse_schema.sql, AFTER the marketing
-- tables (wh_mrk_*) and BEFORE any trailing wh_etl_log / wh_embedding_log
-- tables. Order matters only for readability — there are no cross-table
-- FKs here, each table is self-contained on (business_id, period, ...).
--
-- Sprint: Domain 7 of 11 (Expenses)
-- Step: 4 of 8 (ETL wire-up)
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
-- END EXPENSES DOMAIN TABLES
-- =============================================================================
-- Verification query after deployment (7 tables, 0 rows initially):
--
--   SELECT table_name,
--          (SELECT count(*) FROM information_schema.columns
--           WHERE table_name = t.table_name) AS columns
--   FROM information_schema.tables t
--   WHERE table_name LIKE 'wh_exp_%'
--   ORDER BY table_name;
--
-- Expected result:
--   wh_exp_category_breakdown          | 15
--   wh_exp_category_location_cross     | 11
--   wh_exp_location_breakdown          | 12
--   wh_exp_monthly_summary             | 21
--   wh_exp_payment_type_breakdown      |  9
--   wh_exp_staff_attribution           |  8
--   wh_exp_subcategory_breakdown       | 10
-- =============================================================================


-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: 2026_04_22_promos_warehouse.sql
-- Domain:    Promos (Domain 8)
-- Purpose:   Warehouse tables to land promo redemption data extracted from
--            the analytics backend. Read by doc_generator.promos.py to
--            produce 6 chunk types for embedding into pgvector.
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Conventions inherited from prior domain migrations:
--   • business_id column on every table (tenant isolation enforced at write+read)
--   • period_start as DATE column, NULLABLE for catalog-style rows (Lesson 3)
--   • All currency as NUMERIC(20, 2) — matches API spec types
--   • Composite indexes on (business_id, period_start) for query performance
--   • generated_at TIMESTAMP for staleness checks
-- ═══════════════════════════════════════════════════════════════════════════════


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


-- ── Sanity: rollback notes ───────────────────────────────────────────────────
-- To rollback this migration:
--   DROP TABLE IF EXISTS wh_promo_catalog_health;
--   DROP TABLE IF EXISTS wh_promo_location_codes;
--   DROP TABLE IF EXISTS wh_promo_locations;
--   DROP TABLE IF EXISTS wh_promo_codes;
--   DROP TABLE IF EXISTS wh_promo_monthly;