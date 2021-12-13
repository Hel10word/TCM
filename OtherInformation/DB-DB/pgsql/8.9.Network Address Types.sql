
-- https://www.postgresql.org/docs/13/datatype-net-types.html

CREATE TABLE network_address_types_pgsql (
  "pgcidr" cidr,
  "pginet" inet,
  "pgmacaddr" macaddr,
  "pgmacaddr8" macaddr8
)
;

insert INTO network_address_types_pgsql values ('true');