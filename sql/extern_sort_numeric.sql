CREATE TEMP TABLE numeric_test (i numeric);

-- Seed random number generator to make outcome deterministic.
SELECT setseed(0.5);

INSERT INTO numeric_test SELECT random() * 100000000 FROM generate_series(0, 5000000);

-- special cases
INSERT INTO numeric_test SELECT exp(0.0);
INSERT INTO numeric_test SELECT exp(1.0);
INSERT INTO numeric_test SELECT exp(1.0::numeric(71,70));
INSERT INTO numeric_test SELECT 0.0 ^ 0.0;
INSERT INTO numeric_test SELECT (-12.34) ^ 0.0;
INSERT INTO numeric_test SELECT 12.34 ^ 0.0;
INSERT INTO numeric_test SELECT 0.0 ^ 12.34;

SET maintenance_work_mem = '1MB';
CREATE INDEX idx_numeric ON numeric_test(i);
SELECT bt_index_parent_check('idx_numeric');
SET maintenance_work_mem = '5MB';
REINDEX INDEX idx_numeric;
SELECT bt_index_parent_check('idx_numeric');
-- Only this final REINDEX requires just one merge pass:
SET maintenance_work_mem = '20MB';
REINDEX INDEX idx_numeric;
SELECT bt_index_parent_check('idx_numeric');

-- drop the temporary table
DROP TABLE numeric_test;
