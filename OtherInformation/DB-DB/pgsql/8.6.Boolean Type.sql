
-- https://www.postgresql.org/docs/13/datatype-boolean.html

CREATE TABLE boolean_types_pgsql (
  "pgboolean" boolean
)
;

insert INTO boolean_types_pgsql values ('true');
insert INTO boolean_types_pgsql values ('yes');
insert INTO boolean_types_pgsql values ('on');
insert INTO boolean_types_pgsql values ('1');

insert INTO boolean_types_pgsql values ('false');
insert INTO boolean_types_pgsql values ('no');
insert INTO boolean_types_pgsql values ('off');
insert INTO boolean_types_pgsql values ('0');

select *,coalesce((pgboolean::boolean)::int,0) as pgboolean from boolean_types_pgsql;

-- https://www.postgresql.org/docs/13/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL