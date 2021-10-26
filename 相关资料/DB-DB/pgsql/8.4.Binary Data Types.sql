
-- https://www.postgresql.org/docs/13/datatype-binary.html

CREATE TABLE binary_types_pgsql (
  "pgbytea" bytea
)
;

insert INTO binary_types_pgsql values ('\000'::bytea);
insert INTO binary_types_pgsql values (''''::bytea);
insert INTO binary_types_pgsql values ('\\'::bytea);
insert INTO binary_types_pgsql values ('\001'::bytea);

select encode(pgbytea,'hex'),pgbytea from binary_types_pgsql;

insert INTO binary_types_pgsql values ('\134'::bytea);
insert INTO binary_types_pgsql values ('\001'::bytea);
insert INTO binary_types_pgsql values ('\176'::bytea);

select encode(pgbytea,'escape'),pgbytea from binary_types_pgsql;