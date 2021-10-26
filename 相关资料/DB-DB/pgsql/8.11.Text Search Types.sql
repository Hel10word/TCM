
-- https://www.postgresql.org/docs/13/datatype-textsearch.html

CREATE TABLE text_search_types_pgsql (
  "pgtsvector" tsvector,
  "pgtsquery" tsquery
)
;


INSERT INTO text_search_types_pgsql VALUES ('a fat cat sat on a mat and ate a fat rat','fat & rat');