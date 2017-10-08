/* amcheck_next--1--2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck_next UPDATE TO '2'" to load this file. \quit

--
-- bt_index_check()
--
DROP FUNCTION bt_index_check(regclass);
CREATE FUNCTION bt_index_check(index regclass,
    heapallindexed boolean DEFAULT false)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_check_next'
LANGUAGE C STRICT;

--
-- bt_index_parent_check()
--
DROP FUNCTION bt_index_parent_check(regclass);
CREATE FUNCTION bt_index_parent_check(index regclass,
    heapallindexed boolean DEFAULT false)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_parent_check_next'
LANGUAGE C STRICT;

-- Don't want these to be available to public
REVOKE ALL ON FUNCTION bt_index_check(regclass, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check(regclass, boolean) FROM PUBLIC;
