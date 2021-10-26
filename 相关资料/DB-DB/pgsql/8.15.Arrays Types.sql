
-- https://www.postgresql.org/docs/13/arrays.html

CREATE TABLE arrays_types_pgsql (
  "pgtext" text,
  "pgintefer_x" integer[],
  "pgtext_xx" text[][],
    "pginteger_33" integer[3][3]
)
;

INSERT INTO arrays_types_pgsql VALUES ('test_one','{1,100,1000}','{{"abc","def"},{"abc2","def2"}}','{{00,01,02},{10,11,12},{20,21,22}}');

INSERT INTO arrays_types_pgsql VALUES ('test_two','{2,222,456546,222222}','{{"2222222","2222"},{"def3","hij2"},{"def3","hij2"},{"def3","hij2"}}','{{99,01,02},{10,11,12},{10,11,12}}');
