CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT PRIMARY KEY,
    email TEXT,
    created_at TIMESTAMP,
    plan TEXT
);

CREATE TABLE IF NOT EXISTS raw.subscriptions (
    subscription_id TEXT PRIMARY KEY,
    customer_id TEXT,
    start_date DATE,
    end_date DATE,
    status TEXT,
    mrr NUMERIC,
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id TEXT PRIMARY KEY,
    customer_id TEXT,
    paid_at TIMESTAMP,
    amount NUMERIC,
    currency TEXT,
    status TEXT,
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS raw.product_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT,
    event_ts TIMESTAMP,
    event_name TEXT,
    page TEXT,
    device TEXT,
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);