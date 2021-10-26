
-- https://www.postgresql.org/docs/13/datatype-datetime.html

CREATE TABLE datatime_types_pgsql (
  "pgtimestamp" timestamp,
  "pgtimestampwtz" timestamp with time zone,
  "pgdate" date,
  "pgtime" time,
  "pgtimewtz" time with time zone,
  "pginterval" interval
)
;