CREATE SCHEMA IF NOT EXISTS crm;

CREATE TABLE IF NOT EXISTS crm.customers (
    customer_id   text PRIMARY KEY,
    company_name  text        NOT NULL,
    plan          text        NOT NULL CHECK (plan IN ('free','pro','enterprise')),
    segment       text,
    signup_date   date        NOT NULL,
    country       text,
    is_active     boolean     NOT NULL DEFAULT true,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- índice pensado para extração incremental por watermark
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON crm.customers (updated_at);
