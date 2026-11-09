-- 02_check.sql: confirms the setup worked.
SELECT slot_name, plugin, active FROM pg_replication_slots;
SELECT count(*) AS customer_rows FROM customers;