-- minimal test, basically just verifying that amcheck works with GiST
CREATE TABLE gist_check AS SELECT point(s,1) c FROM generate_series(1,10000) s;
CREATE INDEX gist_check_idx ON gist_check USING gist(c);
SELECT gist_index_check('gist_check_idx');
