-- 01_setup.sql
-- Creates the source table and the CDC objects.
-- Run once against the database, as a user with replication rights.

-- 1. Source table, exactly as specified in the assignment.
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    full_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(100) UNIQUE NOT NULL,
    status      VARCHAR(20) DEFAULT 'ACTIVE',
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Include all old column values in UPDATE and DELETE events.
--    Without this, a DELETE event would contain only the primary key.
ALTER TABLE customers REPLICA IDENTITY FULL;

-- 3. Keep updated_at accurate on every UPDATE, not only on INSERT.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON customers;
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 4. Publication: the named list of tables to replicate.
--    Required by pgoutput. wal2json reads through the slot, so it does not use it.
--    Kept here to match the assignment's publication/slot requirement.
DROP PUBLICATION IF EXISTS customers_pub;
CREATE PUBLICATION customers_pub FOR TABLE customers;