
-- https://www.postgresql.org/docs/13/datatype-datetime.html

CREATE TABLE datatime_types_pgsql (
  "pgtimestamp" timestamp,
  "pgtimestampwtz" timestamp with time zone,
  "pgdate" date,
  "pgtime" time,
  "pgtimewtz" time with time zone,
  "pginterval" interval
);


INSERT INTO "datatime_types_pgsql" ("pgtimestamp", "pgtimestampwtz", "pgdate", "pgtime", "pgtimewtz", "pginterval") VALUES ('4713-01-01 00:00:00.000001 BC', '4713-01-01 00:05:43.000001+08:05:43 BC', '4713-01-01 BC', '00:00:00', '00:00:00+15:59', '-178000000 years');
INSERT INTO "datatime_types_pgsql" ("pgtimestamp", "pgtimestampwtz", "pgdate", "pgtime", "pgtimewtz", "pginterval") VALUES ('294276-12-31 23:59:59.999999', '294276-12-31 23:59:59.999999+08', '5874897-12-31', '24:00:00', '24:00:00-15:59', '178000000 years');
INSERT INTO "datatime_types_pgsql" ("pgtimestamp", "pgtimestampwtz", "pgdate", "pgtime", "pgtimewtz", "pginterval") VALUES ('1000-01-01 00:00:00.000001', '1970-01-01 08:00:01.000001+08', '1000-01-01', NULL, '00:00:00+15:59', NULL);
INSERT INTO "datatime_types_pgsql" ("pgtimestamp", "pgtimestampwtz", "pgdate", "pgtime", "pgtimewtz", "pginterval") VALUES ('9999-12-31 23:59:59.999999', '2038-01-19 11:14:07.999999+08', '9999-12-31', NULL, '24:00:00-15:59', NULL);

