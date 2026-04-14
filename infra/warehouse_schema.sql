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