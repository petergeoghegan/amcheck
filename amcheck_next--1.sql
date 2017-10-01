/* amcheck_next--1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION amcheck_next" to load this file. \quit

--
-- bt_index_check()
--
CREATE FUNCTION bt_index_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_check_next'
LANGUAGE C STRICT;

--
-- bt_index_parent_check()
--
CREATE FUNCTION bt_index_parent_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_parent_check_next'
LANGUAGE C STRICT;

-- Don't want these to be available to public
REVOKE ALL ON FUNCTION bt_index_check(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check(regclass) FROM PUBLIC;
