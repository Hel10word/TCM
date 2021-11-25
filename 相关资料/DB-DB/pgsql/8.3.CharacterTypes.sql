
-- https://www.postgresql.org/docs/13/datatype-character.html

CREATE TABLE "character_types_pgsql" (
  "pgcharactervarying" character varying,
  "pgvarchar" varchar,
  "pgcharacter" character,
  "pgchar" char,
  "pgtext" text 
)
;

--  !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[ ]、^_·——`abcdefghijklmnopqrstuvwxyz{|}~
-- ' !"#$%&''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[ ]
-- 、^_·——`abcdefghijklmnopqrstuvwxyz{|}~'




INSERT INTO "character_types_pgsql" ("pgcharactervarying", "pgvarchar", "pgcharacter", "pgchar", "pgtext") VALUES (' !"#$%&''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[ ]、^_·——`abcdefghijklmnopqrstuvwxyz{|}~', ' !"#$%&''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[ ]、^_·——`abcdefghijklmnopqrstuvwxyz{|}~', '1', '1', 'SQL defines two primary character types: character varying(n) and character(n), where n is a positive integer. Both of these types can store strings up to n characters (not bytes) in length. An attempt to store a longer string into a column of these types will result in an error, unless the excess characters are all spaces, in which case the string will be truncated to the maximum length. (This somewhat bizarre exception is required by the SQL standard.) If the string to be stored is shorter than the declared length, values of type character will be space-padded; values of type character varying will simply store the shorter string.If one explicitly casts a value to character varying(n) or character(n), then an over-length value will be truncated to n characters without raising an error. (This too is required by the SQL standard.)The notations varchar(n) and char(n) are aliases for character varying(n) and character(n), respectively. character without length specifier is equivalent to character(1). If character varying is used without length specifier, the type accepts strings of any size. The latter is a PostgreSQL extension.In addition, PostgreSQL provides the text type, which stores strings of any length. Although the type text is not in the SQL standard, several other SQL database management systems have it as well.Values of type character are physically padded with spaces to the specified width n, and are stored and displayed that way. However, trailing spaces are treated as semantically insignificant and disregarded when comparing two values of type character. In collations where whitespace is significant, this behavior can produce unexpected results; for example SELECT ''a ''::CHAR(2) collate "C" < E''a\n''::CHAR(2) returns true, even though C locale would consider a space to be greater than a newline. Trailing spaces are removed when converting a character value to one of the other string types. Note that trailing spaces are semantically significant in character varying and text values, and when using pattern matching, that is LIKE and regular expressions.The characters that can be stored in any of these data types are determined by the database character set, which is selected when the database is created. Regardless of the specific character set, the character with code zero (sometimes called NUL) cannot be stored. For more information refer to Section 23.3.The storage requirement for a short string (up to 126 bytes) is 1 byte plus the actual string, which includes the space padding in the case of character. Longer strings have 4 bytes of overhead instead of 1. Long strings are compressed by the system automatically, so the physical requirement on disk might be less. Very long values are also stored in background tables so that they do not interfere with rapid access to shorter column values. In any case, the longest possible character string that can be stored is about 1 GB. (The maximum value that will be allowed for n in the data type declaration is less than that. It wouldn''t be useful to change this because with multibyte character encodings the number of characters and bytes can be quite different. If you desire to store long strings with no specific upper limit, use text or character varying without a length specifier, rather than making up an arbitrary length limit.)');


INSERT INTO "character_types_pgsql" ("pgcharactervarying", "pgvarchar", "pgcharacter", "pgchar", "pgtext") VALUES (''' !"#$%&''''()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\[ ]

、^_·——`abcdefghijklmnopqrstuvwxyz{|}~''', 2, 3, 4, 5);