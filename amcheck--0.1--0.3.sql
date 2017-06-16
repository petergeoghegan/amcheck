/* amcheck--0.1--0.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION amcheck" to load this file. \quit

--
-- No change in function signature, but must revoke all privileges on
-- functions from public due to adopting new permissions model (no direct
-- enforcement, so functions can be used by non-superusers).
--
REVOKE ALL ON FUNCTION bt_index_check(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check(regclass) FROM PUBLIC;
