
-- https://www.postgresql.org/docs/13/datatype-uuid.html

CREATE TABLE uuid_types_pgsql (
  "pguuid" uuid
)
;


insert INTO uuid_types_pgsql values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
insert INTO uuid_types_pgsql values ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
insert INTO uuid_types_pgsql values ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}');
insert INTO uuid_types_pgsql values ('a0eebc999c0b4ef8bb6d6bb9bd380a11');
insert INTO uuid_types_pgsql values ('a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11');
insert INTO uuid_types_pgsql values ('{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}');