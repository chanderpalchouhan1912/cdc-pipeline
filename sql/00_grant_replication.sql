-- 00_grant_replication.sql
-- Gives the admin role permission to create replication slots.
ALTER ROLE pgadmin WITH REPLICATION;