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

CREATE TABLE fatest(
	iary int4[],
	tary text[]
);

INSERT INTO fatest (iary) VALUES
(ARRAY[3, 5, 10]),
(ARRAY[-100, NULL, NULL, NULL, 5]),
('[0:1]={2,3}'),
(ARRAY[[1,2],[2,3]]);

SELECT maparray(iary, 'int4inc'), maparray(iary, 'int4um') FROM fatest;

SELECT reducearray(iary, 'int4pl') FROM fatest;
