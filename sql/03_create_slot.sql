-- 03_create_slot.sql
-- Creates the replication slot. Must run in its own execution,
-- because a slot cannot be created inside a transaction that has written data.
SELECT pg_create_logical_replication_slot('customers_cdc_slot', 'wal2json');