CREATE TEMP TABLE bytea_test (i bytea);

-- Seed random number generator to make outcome deterministic.
SELECT setseed(0.5);

INSERT INTO bytea_test SELECT decode(repeat(md5(random()::text), (random() * 100)::int4), 'hex')
      FROM generate_series(0, 100000);

SET maintenance_work_mem = '1MB';
CREATE INDEX idx_bytea ON bytea_test(i);
SELECT bt_index_parent_check('idx_bytea');
SET maintenance_work_mem = '3MB';
REINDEX INDEX idx_bytea;
SELECT bt_index_parent_check('idx_bytea');
SET maintenance_work_mem = '5MB';
REINDEX INDEX idx_bytea;
SELECT bt_index_parent_check('idx_bytea');
SET maintenance_work_mem = '20MB';
REINDEX INDEX idx_bytea;
SELECT bt_index_parent_check('idx_bytea');

-- drop the temporary table
DROP TABLE bytea_test;
