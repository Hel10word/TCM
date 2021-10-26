
-- https://www.postgresql.org/docs/13/datatype-geometric.html

CREATE TABLE geometric_types_pgsql (
    pgpoint point,
    pgline line,
    pglseg lseg,
    pgbox box,
    pgpath path,
    pgpolygon polygon,
    pgcircle circle
);
