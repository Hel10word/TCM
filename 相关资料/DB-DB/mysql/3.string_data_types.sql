
-- https://dev.mysql.com/doc/refman/5.7/en/string-type-syntax.html

CREATE TABLE  string_types_mysql(
    mychar CHAR(255),
    myvarchar VARCHAR(65535),
    mybinary BINARY(255),
    myvarbinary VARBINARY(65535),
    myblob BLOB,
    mytext TEXT,
    mymediumblob MEDIUMBLOB,
    mymediumtext MEDIUMTEXT,
    mylongblob LONGBLOB,
    mylongtext LONGTEXT,
    myenum ENUM('enum_one','enum_two'),
    myset SET('set_one','set_two','set_three')
);

-- !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~

INSERT INTO `string_types_mysql` (`mychar`, `myvarchar`, `mybinary`, `myvarbinary`, `myblob`, `mytext`, `mymediumblob`, `mymediumtext`, `mylongblob`, `mylongtext`, `myenum`, `myset`) VALUES ('!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', '!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', NULL, NULL, NULL, 'SQL defines two primary character types: character varying(n) and character(n), where n is a positive integer. Both of these types can store strings up to n characters (not bytes) in length. An attempt to store a longer string into a column of these types will result in an error, unless the excess characters are all spaces, in which case the string will be truncated to the maximum length. (This somewhat bizarre exception is required by the SQL standard.) If the string to be stored is shorter than the declared length, values of type character will be space-padded; values of type character varying will simply store the shorter string.', NULL, 'SQL defines two primary character types: character varying(n) and character(n), where n is a positive integer. Both of these types can store strings up to n characters (not bytes) in length. An attempt to store a longer string into a column of these types will result in an error, unless the excess characters are all spaces, in which case the string will be truncated to the maximum length. (This somewhat bizarre exception is required by the SQL standard.) If the string to be stored is shorter than the declared length, values of type character will be space-padded; values of type character varying will simply store the shorter string.', NULL, 'SQL defines two primary character types: character varying(n) and character(n), where n is a positive integer. Both of these types can store strings up to n characters (not bytes) in length. An attempt to store a longer string into a column of these types will result in an error, unless the excess characters are all spaces, in which case the string will be truncated to the maximum length. (This somewhat bizarre exception is required by the SQL standard.) If the string to be stored is shorter than the declared length, values of type character will be space-padded; values of type character varying will simply store the shorter string.', 'enum_one', 'set_one');

INSERT INTO `string_types_mysql` (`mychar`, `myvarchar`, `mybinary`, `myvarbinary`, `myblob`, `mytext`, `mymediumblob`, `mymediumtext`, `mylongblob`, `mylongtext`, `myenum`, `myset`) VALUES ('!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', '!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', NULL, NULL, NULL, '!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', NULL, '!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', NULL, '!\\\"#$%&\\\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~', 'enum_two', 'set_one,set_two,set_three');
