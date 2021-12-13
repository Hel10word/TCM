
-- https://www.postgresql.org/docs/13/rowtypes.html

CREATE TYPE complex AS (
    r       double precision,
    i       double precision
);
CREATE TYPE inventory_item AS (
    name            text,
    supplier_id     integer,
    price           numeric
);
CREATE TABLE composite_types_pgsql (
    item      inventory_item,
    count     integer
);

INSERT INTO composite_types_pgsql VALUES (ROW('fuzzy dice', 42, 1.99), 1000);