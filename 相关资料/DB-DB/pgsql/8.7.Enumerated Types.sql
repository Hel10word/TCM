
-- https://www.postgresql.org/docs/13/datatype-enum.html

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE enumerated_types_pgsql (
    pgtext_name text,
    pgenumerated_mood mood
);

INSERT INTO enumerated_types_pgsql VALUES ('Moe', 'happy');

SELECT * FROM enumerated_types_pgsql WHERE pgenumerated_mood = 'happy';