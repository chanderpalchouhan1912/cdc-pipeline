-- 04_demo_changes.sql
-- One INSERT, one UPDATE, one DELETE, so the listener can show all three operations.
INSERT INTO customers (full_name, email) VALUES ('Asha Rao', 'asha.rao@example.com');
UPDATE customers SET status = 'INACTIVE' WHERE email = 'asha.rao@example.com';
DELETE FROM customers WHERE email = 'asha.rao@example.com';