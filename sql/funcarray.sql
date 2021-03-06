SET client_min_messages = warning;
\set ECHO none
\i funcarray.sql
\set ECHO all
RESET client_min_messages;

SELECT maparray(ARRAY[1,3,5], 'int4inc'::regproc);

SELECT maparray(maparray(ARRAY[-10,10,0]::int2[], 'int2abs'), 'int2um');

SELECT maparray(ARRAY[200000000, 0]::int8[], 'int8inc'::regproc);

CREATE FUNCTION _floatdiv2(float) RETURNS float AS $$
SELECT $1 / 2.0
$$ LANGUAGE 'SQL' IMMUTABLE STRICT;

SELECT maparray(ARRAY[1.5::float,0.3,1], '_floatdiv2');

SELECT maparray(ARRAY['test', 'McDonald']::text[], 'upper');

SELECT reducearray(ARRAY['this', 'is', 'a', 'pen'], 'textcat');

SELECT filterarray(ARRAY[3,0,100,-1,0], 'bool');

CREATE FUNCTION _iseven(int) RETURNS bool AS $$
SELECT $1 % 2 = 0
$$ LANGUAGE 'SQL' IMMUTABLE;

SELECT filterarray(ARRAY[3,0,100,NULL,-1,0], '_iseven');

CREATE TABLE fatest(
	iary int4[],
	mapproc regproc,
	reduceproc regproc
);

INSERT INTO fatest (iary, mapproc, reduceproc) VALUES
(ARRAY[3, 5, 10], 'int4inc', 'int4pl'),
(ARRAY[-100, NULL, NULL, NULL, 5], 'int4um', 'int4mul'),
('[0:1]={2,3}', 'int4inc', 'int4pl'),
(ARRAY[[1,2],[2,3]], 'int4um', 'int4mul');

SELECT maparray(iary, 'int4inc'), maparray(iary, mapproc) FROM fatest;

SELECT reducearray(iary, reduceproc) FROM fatest;

CREATE TABLE fatest2(
	tary text[],
	proc regproc
);

INSERT INTO fatest2(tary, proc) VALUES
(ARRAY['traffic', 'country', 'united kingdom'], 'upper'),
(ARRAY[NULL, 'WHO', 'Traffic', 'President', 'Long Horn', NULL], 'lower'),
('[0:1]={enable_nestloop, enable_mergejoin}', 'current_setting');

SELECT maparray(tary, proc) FROM fatest2;
