
-- https://www.postgresql.org/docs/13/datatype-money.html

CREATE TABLE monetart_types_pgsql (
  "pgmoney" money
)
;


INSERT INTO "monetart_types_pgsql" ("pgmoney") VALUES ('-$92,233,720,368,547,758.08');
INSERT INTO "monetart_types_pgsql" ("pgmoney") VALUES ('$92,233,720,368,547,758.07');


select *,pgmoney::money::numeric as pgmoney3 from monetart_types_pgsql
