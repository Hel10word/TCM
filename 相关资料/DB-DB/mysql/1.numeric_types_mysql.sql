
-- https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html

CREATE TABLE  numeric_types_mysql(
    myinteger INTEGER,
    mysmallint SMALLINT,
    mydecimal DECIMAL(65,30),
    mynumeric NUMERIC(65,30),
    myfloat FLOAT,
    myreal REAL,
    mydouble_percision DOUBLE PRECISION,
    myint INT,
    mydec DEC(65,30),
    myfixed FIXED(65,30),
    mydouble DOUBLE,
    mybit BIT(1),
    mytinyint TINYINT,
    mymediumint MEDIUMINT,
    mybigint BIGINT,
    mybool BOOL,
    myboolean BOOLEAN
);


select BIN('01');

INSERT INTO `numeric_types_mysql` (`myinteger`, `mysmallint`, `mydecimal`, `mynumeric`, `myfloat`, `myreal`, `mydouble_percision`, `myint`, `mydec`, `myfixed`, `mydouble`, `mybit`, `mytinyint`, `mymediumint`, `mybigint`, `mybool`, `myboolean`) VALUES (-2147483648, -32768, 0.000000000000000000000000000001, 0.000000000000000000000000000001, 0.000000000000000000000000000001, 0.000000000000000000000000000001, 0.000000000000000000000000000001, -2147483648, 0.000000000000000000000000000001, 0.000000000000000000000000000001, 0.000000000000000000000000000001, b'0', -128, -8388608, -9223372036854775808, 0, 0);
INSERT INTO `numeric_types_mysql` (`myinteger`, `mysmallint`, `mydecimal`, `mynumeric`, `myfloat`, `myreal`, `mydouble_percision`, `myint`, `mydec`, `myfixed`, `mydouble`, `mybit`, `mytinyint`, `mymediumint`, `mybigint`, `mybool`, `myboolean`) VALUES (2147483647, 32767, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, 2147483647, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, 9999999.999999999999999999999999999999, b'1', 127, 8388607, 9223372036854775807, 1, 1);