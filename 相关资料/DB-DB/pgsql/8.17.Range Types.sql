
-- https://www.postgresql.org/docs/13/rangetypes.html

CREATE TABLE range_types_pgsql (
    pgtext text, 
    pgint4range int4range,
    pgint8range int8range,
    pgnumrange numrange,
    pgtsrange tsrange,
    pgtstzrange tstzrange,
    pgdaterange daterange
);
