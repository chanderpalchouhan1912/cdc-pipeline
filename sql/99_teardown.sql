-- 99_teardown.sql
-- Removes CDC objects. Run before you finish, so no slot keeps WAL growing.
SELECT pg_drop_replication_slot('customers_cdc_slot')
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'customers_cdc_slot');

DROP PUBLICATION IF EXISTS customers_pub;
DROP TRIGGER IF EXISTS trg_customers_updated_at ON customers;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TABLE IF EXISTS customers;