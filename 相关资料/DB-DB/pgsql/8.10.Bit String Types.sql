
-- https://www.postgresql.org/docs/13/datatype-bit.html

CREATE TABLE bit_string_types_pgsql (
  "pgbit_1" BIT(1),
  "pgbit_3" BIT(3),
  "pgbitvarying_1" BIT VARYING(1),
  "pgbitvarying_5" BIT VARYING(5)
)
;

INSERT INTO bit_string_types_pgsql VALUES (B'1',B'101',B'1', B'00');
INSERT INTO bit_string_types_pgsql VALUES (B'0',B'010',B'0', B'01010');