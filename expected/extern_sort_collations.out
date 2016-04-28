-- Perform tests of external sorting collated text.  Only test at most 10
-- available collations, to make sure that the tests complete in a reasonable
-- amount of time.
DO $x$
DECLARE
  r record;
  index_build_mem int4;
BEGIN
	FOR r IN SELECT oid, collname FROM pg_collation
    WHERE collencoding = (SELECT encoding FROM pg_database WHERE datname = current_database())
    ORDER BY oid LIMIT 10 LOOP

  EXECUTE format($y$CREATE TEMP TABLE text_test_collation (i text COLLATE "%s");$y$, r.collname);

  -- Seed random number generator so that each collation that happens to be
  -- available on the system consistently sorts the same strings, using the
  -- same maintenance_work_mem setting.  In theory, varying these makes the
  -- tests more likely to catch problems.
  PERFORM setseed(get_byte(decode(md5(r.collname), 'hex'), 5) / 256.0);
  -- Don't actually output collation names
  -- RAISE NOTICE '%', r.collname;

  -- Make sure MD5 strings contain some space characters, and a doublequote, so
  -- multiple "weight levels" are represented.  Typically, OS strcoll()
  -- implementations follow the conventions of the standard Unicode collation
  -- algorithm, and primarily weight alphabetical ordering when comparing
  -- strings.
  INSERT INTO text_test_collation
      SELECT replace(replace(md5(random()::text), '4', ' '), '9', '"')
      FROM generate_series(0, 100000);

  -- Set maintenance_work_mem  to at least mimimum value, 1MB, and at most 10MB
  -- to test external sorting:
  index_build_mem := (SELECT greatest((random() * 1024 * 10)::int4, 1024));
  -- Don't actually output pseudo-random maintenance_work_mem:
  -- RAISE NOTICE '%', index_build_mem;
  EXECUTE 'SET maintenance_work_mem = ' || index_build_mem;
  CREATE INDEX idx_text ON text_test_collation(i);
  PERFORM bt_index_parent_check('idx_text');
  -- drop the temporary table
  DROP TABLE text_test_collation;
	END LOOP;
END;
$x$;
