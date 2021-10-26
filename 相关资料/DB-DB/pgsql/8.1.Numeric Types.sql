
-- https://www.postgresql.org/docs/13/datatype-numeric.html

CREATE TABLE numeric_types_pgsql (
  "pgsmallint" smallint,
  "pginteger" integer,
  "pgbigint" bigint,
  "pgdecimal " decimal ,
  "pgnumeric " numeric ,
  "pgreal " real ,
  "pgdouble_precision" double precision,
  "pgsmallserial" smallserial,
  "pgserial " serial ,
  "pgbigserial" bigserial
)
;

INSERT INTO "numeric_types_pgsql" ("pgsmallint", "pginteger", "pgbigint", "pgdecimal ", "pgnumeric ", "pgreal ", "pgdouble_precision", "pgsmallserial", "pgserial ", "pgbigserial") VALUES (-32768, -2147483648, -9223372036854775808, '123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789', '0.12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678', '0.123456', '0.123456789012345', 1, 1, 1);
INSERT INTO "numeric_types_pgsql" ("pgsmallint", "pginteger", "pgbigint", "pgdecimal ", "pgnumeric ", "pgreal ", "pgdouble_precision", "pgsmallserial", "pgserial ", "pgbigserial") VALUES (32767, 2147483647, 9223372036854775807, '0.12345678912345678912345678912345678912345678912345678912345678912345678912345678912345678912345678', '123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789', '1e-06', '1e-15', 32767, 2147483647, 9223372036854775807);