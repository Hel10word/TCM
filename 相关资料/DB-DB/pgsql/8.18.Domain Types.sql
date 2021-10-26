
-- https://www.postgresql.org/docs/13/domains.html

CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE TABLE domain_types_pgsql (id posint);


INSERT INTO domain_types_pgsql VALUES(1);   -- works
INSERT INTO domain_types_pgsql VALUES(-1);  -- fails