







## PgSQL 2 MySQL

[refer to]:https://www.postgresql.org/docs/13/datatype.html



### ==numeric_types_pgsql==

https://www.postgresql.org/docs/13/datatype-numeric.html

| PgSQL Data Type  | Range                                                        | TCM Type | MySQL    |
| ---------------- | ------------------------------------------------------------ | -------- | -------- |
| smallint         | -32768 to +32767                                             | INT16    | SMALLINT |
| integer          | -2147483648 to +2147483647                                   | INT32    | INT      |
| bigint           | -9223372036854775808 to +9223372036854775807                 | INT64    | BIGINT   |
| ==decimal==      | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point | DECIMAL  | DECIMAL  |
| ==numeric==      | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point | DECIMAL  | DECIMAL  |
| real             | 6 decimal digits precision                                   | FLOAT32  | FLOAT    |
| double precision | 15 decimal digits precision                                  | FLOAT64  | DOUBLE   |
| smallserial      | 1 to 32767                                                   | INT16    | SMALLINT |
| serial           | 1 to 2147483647                                              | INT32    | INT      |
| bigserial        | 1 to 9223372036854775807                                     | INT64    | BIGINT   |

[mysql-decimal ]:https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html

-   ==decimal== 与 ==numeric== 类型 Mapping 到 MySQL DECIMAL，若数据整体长度超过 **65位** 或小数点后面大于 **30位** 会存在精度丢失。



### monetart_types_pgsql

https://www.postgresql.org/docs/13/datatype-money.html

| PgSQL Data | Range                                          | TCM Type | MySQL         |
| ---------- | ---------------------------------------------- | -------- | ------------- |
| money      | -92233720368547758.08 to +92233720368547758.07 | DECIMAL  | DECIMAL(65,2) |

`-92233720368547758.08` save in money data type is `-$92,233,720,368,547,758.08`，导出时通过类型转换解决。





### character_types_pgsql

https://www.postgresql.org/docs/13/datatype-character.html

| PgSQL Data Type          | Description                | TCM Type | MySQL    |
| ------------------------ | -------------------------- | -------- | -------- |
| character varying(*`n`*) | variable-length with limit | STRING   | VARCHAR  |
| varchar(*`n`*)           | variable-length with limit | STRING   | VARCHAR  |
| character(*`n`*)         | fixed-length, blank padded | STRING   | VARCHAR  |
| char(*`n`*)              | fixed-length, blank padded | STRING   | VARCHAR  |
| text                     | variable unlimited length  | TEXT     | LONGTEXT |

-   需要注意 text 类型，在 pgsql 中是可以存放无限长度的,转到 MySQL 中会存在丢弃末尾长度。



### ==binary_types_pgsql==

https://www.postgresql.org/docs/13/datatype-binary.html

| PgSQL Data | Description                   | TCM Type | MySQL     |
| ---------- | ----------------------------- | -------- | --------- |
| ==bytea==  | variable-length binary string | BYTES    | VARBINARY |

-   由于 pgsql 在对==bytea==字段导出时 默认使用 Hex（十六进制） 进行编码保存，因此写入 MySQL 的都是 Hex（十六进制）类型的值，且MySQL有行限制  [参考](https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html) 因此默认导入为VARBINARY（8192）类型。







### ==datatime_types_pgsql==

https://www.postgresql.org/docs/13/datatype-datetime.html

| PgSQL Data               | Range | TCM Type | MySQL |
| ------------------------ | ----------------------- | -------- | ----- |
| timestamp                | 4713 BC ~ 294276 AD     | TIMESTAMP | TIMESTAMP |
| timestamp with time zone | 4713 BC ~ 294276 AD     | TIMESTAMP | TIMESTAMP |
| timestamp without time zone | 4713 BC ~ 294276 AD     | TIMESTAMP | TIMESTAMP |
| date                     | 4713 BC ~ 5874897 AD    | DATE | DATE |
| time                     | 00:00:00 ~ 24:00:00               | TIME |TIME|
| time with time zone      | 00:00:00+1559 ~ 24:00:00-1559 | TIME | TIME |
| time without time zone   | 00:00:00+1559 ~ 24:00:00-1559 | TIME | TIME |
| ==interval==       | -178000000 years ~ 178000000 years | STRING | VARCHAR(255) |

[mysql-timestamp]:https://dev.mysql.com/doc/refman/5.7/en/timestamp-initialization.html

-   MySQL ==TIMESTAMP== 类型存储范围为 1970-01-01 00:00:01.000000 UTC ~ 2038-01-19 03:14:07.999999 UTC。若 PgSQL 中的数据大于此范围，则到 MySQL 中为 `0000-00-00 00:00:00.000000`。
-   MySQL ==DATE== 类型存储范围为 1000-01-01~9999-12-31。若PgSQL中的数据大于此范围，则到 MySQL 中为 `0000-00-00`。
-   MySQL ==TIME== 类型存储范围为 00:00:00.000000~24:00:00.000000。若PgSQL中的数据大于此范围，则到 MySQL 中为 `00:00:00.000000`。
-   由于 MySQL 中没有找到与 ==interval== 对应的时间类型，因此用 VARCHAR(255) 存储。





### boolean_types_pgsql

https://www.postgresql.org/docs/13/datatype-boolean.html

| PgSQL Data | True/False in PgSQL                                          | TCM Type | MySQL      |
| ---------- | ------------------------------------------------------------ | -------- | ---------- |
| boolean    | ('true','yes','on','1',1) => t; ('false','no','off','0',0)=>f | BOOLEAN  | TINYINT(1) |

[MySQL-boolean]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html

-   PgSQL 存储 boolean 采用的 t/f ，而MySQL 都识别为0，需要通过转换为 1/0 再插入MySQL；

select *,coalesce((pgboolean::boolean)::int,0) as pgboolean from boolean_types_pgsql；







### ==enumerated_types_pgsql==

https://www.postgresql.org/docs/13/datatype-enum.html

| PgSQL Data | TCM Type | MySQL |
| ---------- | -------- | ----- |
| ENUM       |          |       |

```sql
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE enumerated_types_pgsql (
    pgtext_name text,
    pgenumerated_mood mood
);
```

根据 Metadata 查询得知。

ColumnName='pgenumerated_mood', DataType='USER-DEFINED'

-   该类型为用户自定义类型，**暂时不支持该类型**。





### geometric_types_pgsql

https://www.postgresql.org/docs/13/datatype-geometric.html

| PgSQL Data | Description                                 | TCM Type | MySQL    |
| ---------- | ------------------------------------------- | -------- | -------- |
| point      | Point on a plane                            | TEXT     | LONGTEXT |
| line       | Infinite line                               | TEXT     | LONGTEXT |
| lseg       | Finite line segment                         | TEXT     | LONGTEXT |
| box        | Rectangular box                             | TEXT     | LONGTEXT |
| path       | Closed path (similar to polygon)；Open path | TEXT     | LONGTEXT |
| polygon    | Polygon (similar to closed path)            | TEXT     | LONGTEXT |
| circle     | Circle                                      | TEXT     | LONGTEXT |

-   由于不同数据库对几何数据的支持不同，因此这儿统一转换为长字符串。






### network_address_types_pgsql

https://www.postgresql.org/docs/13/datatype-net-types.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| cidr       | TEXT     | LONGTEXT |
| inet       | TEXT     | LONGTEXT |
| macaddr    | TEXT     | LONGTEXT |
| macaddr8   | TEXT     | LONGTEXT |

-   由于 MySQL 中没有与之对应的 字段类型，因此这儿统一转换为长字符串。





### ==bit_string_types_pgsql==

https://www.postgresql.org/docs/13/datatype-bit.html

| PgSQL Data     | TCM Type | MySQL     |
| -------------- | -------- | --------- |
| BIT[M]         | BYTES    | VARBINARY |
| BIT VARYING[M] | BYTES    | VARBINARY |

-   在PgSQL中查询官方手册，暂未发现 M 的定义范围。
-   由于Mapping 到 MySQL中，为  VARBINARY[M] 0<=M<=21845，**数据过长会丢失**。





### text_search_types_pgsql

https://www.postgresql.org/docs/13/datatype-textsearch.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| tsvector   | TEXT     | LONGTEXT |
| tsquery    | TEXT     | LONGTEXT |

-   由于 MySQL 中没有与之对应的 字段类型，因此这儿统一转换为长字符串。





### uuid_types_pgsql

https://www.postgresql.org/docs/13/datatype-uuid.html

| PgSQL Data | TCM Type | MySQL        |
| ---------- | -------- | ------------ |
| uuid       | STRING   | VARCHAR(255) |

-   默认转为 MySQL : VARCHAR(255)；





### xml_types_pgsql

https://www.postgresql.org/docs/13/datatype-xml.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| xml        | TEXT     | LONGTEXT |

-   PgSQL 中的 XML 类型没有限定长度，因此转为 varchar(255) 可能会溢出，所以将其转换为 LONGTEXT 类型。





### json_types_pgsql

https://www.postgresql.org/docs/13/datatype-json.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| json       | TEXT     | LONGTEXT |
| jsonb      | TEXT     | LONGTEXT |

-   由于 PgSQL 中的 json 类型也可以存储 text 类型的数据，如果在 MySQL 中使用 varchar (255) 存储会有数据丢失，所以将其转换为 LONGTEXT 类型。 







### array_types_pgsql

https://www.postgresql.org/docs/13/arrays.html

| PgSQL Data     | TCM Type | MySQL    |
| -------------- | -------- | -------- |
| integer[]      | TEXT     | LONGTEXT |
| text\[][]      | TEXT     | LONGTEXT |
| integer\[3][3] | TEXT     | LONGTEXT |

-   ColumnName='pgintefer_[]', DataType='ARRAY', Position=2, tableCLoneManageType=null


根据Metadata查询可得知，如论一维数组还是二维数组，查询结果都为： data_type=ARRAY；所以将其转换为 LONGTEXT 类型。









### ==composite_types_pgsql==

https://www.postgresql.org/docs/13/rowtypes.html

```sql
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
```



-   这是一种符合类型，可以自定义一些结构体。
-   根据 Metadata 查出来为 DataType= USER-DEFINED ，因此**不支持此类型**。







### range_types_pgsql

https://www.postgresql.org/docs/13/rangetypes.html

| PgSQL Data | Description                            | TCM Type | MySQL    |
| ---------- | -------------------------------------- | -------- | -------- |
| int4range  | Range of `integer`                     | TEXT     | LONGTEXT |
| int8range  | Range of `bigint`                      | TEXT     | LONGTEXT |
| numrange   | Range of `numeric`                     | TEXT     | LONGTEXT |
| tsrange    | Range of `timestamp without time zone` | TEXT     | LONGTEXT |
| tstzrange  | Range of `timestamp with time zone`    | TEXT     | LONGTEXT |
| daterange  | Range of `date`                        | TEXT     | LONGTEXT |

-   由于 MySQL 中没有与之对应的 字段类型，因此这儿统一转换为长字符串。








### ==domain_types_pgsql==

https://www.postgresql.org/docs/13/domains.html

```sql
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE TABLE domain_types_pgsql (id posint);
INSERT INTO domain_types_pgsql VALUES(1);   -- works
INSERT INTO domain_types_pgsql VALUES(-1);  -- fails
```

-   该类型与其他类型一致，域类型，仅仅是起到限定作用，如上代码，表中的字段类型为 integer ，因此**不支持此类型**。







### object_types_pgsql

https://www.postgresql.org/docs/13/datatype-oid.html

| PgSQL Data    | Description                  | TCM Type | MySQL    |
| ------------- | ---------------------------- | -------- | -------- |
| oid           | numeric object identifier    | TEXT     | LONGTEXT |
| regclass      | relation name                | TEXT     | LONGTEXT |
| regcollation  | collation name               | TEXT     | LONGTEXT |
| regconfig     | text search configuration    | TEXT     | LONGTEXT |
| regdictionary | text search dictionary       | TEXT     | LONGTEXT |
| regnamespace  | namespace name               | TEXT     | LONGTEXT |
| regoper       | operator name                | TEXT     | LONGTEXT |
| regoperator   | operator with argument types | TEXT     | LONGTEXT |
| regproc       | function name                | TEXT     | LONGTEXT |
| regprocedure  | function with argument types | TEXT     | LONGTEXT |
| regrole       | role name                    | TEXT     | LONGTEXT |
| regtype       | data type name               | TEXT     | LONGTEXT |

-   由于 MySQL 中没有与之对应的类型，因此转为 LONGTEXT 类型存储。





### pg_lsn_types_pgsql

https://www.postgresql.org/docs/13/datatype-pg-lsn.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| pg_lsn     | TEXT     | LONGTEXT |

[PgSQL-pg_lsn]:https://www.postgresql.org/docs/13/datatype-pg-lsn.html

-   由于 pg_lsn 是PgSQL的内部数据类型， MySQL 中没有与之对应的类型，因此转为 LONGTEXT 类型存储。









[PgSQL-pseudo]:https://www.postgresql.org/docs/13/datatype-pseudo.html

该类型不是用来修饰字段的，是 PgSQL 中的伪类型，用来修饰函数，本工具暂不考虑。













##  MySQ 2 PgSQL



[refer to]:https://dev.mysql.com/doc/refman/5.7/en/data-types.html

### numeric_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/numeric-types.html

| MySQL Data              | Range                                                        | TCM Type | PgSQL            |
| ----------------------- | ------------------------------------------------------------ | -------- | ---------------- |
| INTEGER                 | -2147483648 ~ 2147483647                                     | INT32    | INT              |
| SMALLINT[(M)]           | -32768 ~ 32767,0<=M<=65535                                   | INT16    | SMALLINT         |
| DECIMAL[(M,D)]          | 1<=M<=65;default 10;  1<=D<=30;default 0;                    | DECIMAL  | DECIMAL          |
| NUMERIC[(M,D)]          | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| FLOAT(p)                | 0<=p<23,results in a 4-byte single-precision FLOAT；24<=p<53,results in a 8-byte double-precision DOUBLE； | FLOAT64  | DOUBLE PRECISION |
| REAL                    | as a synonym for DOUBLE PRECISION                            | FLOAT64  | DOUBLE PRECISION |
| DOUBLE PRECISION[(M,D)] | 1<=M<=65;default 10;  1<=D<=30;default 0;                    | FLOAT64  | DOUBLE PRECISION |
| INT                     | as a synonym for INTEGER                                     | INT32    | INT              |
| DEC                     | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| FIXED[(M,D)]            | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| DOUBLE[(M,D)]           | as a synonym for DOUBLE PRECISION                            | FLOAT64  | DOUBLE PRECISION |
| BIT[M]                  | 1<=M<=64;default 1;                                          | BYTES    | BYTES            |
| TINYINT(1)              |                                                              | BOOLEAN  | BOOLEAN          |
| TINYINT[M]              | 1<=M<=225;default 4;                                         | INT8     | SMALLINT         |
| MEDIUMINT               | -8388608 ~ 8388607                                           | INT32    | INT              |
| BIGINT                  | -2^63^ （-9223372036854775808）~ 2^63^-1（9223372036854775807） | INT64    | BIGINT           |
| BOOL                    | 0 and 1； the values `TRUE` and `FALSE` are merely aliases for `1` and `0`. | BOOLEAN  | BOOLEAN          |
| BOOLEAN                 | 0 and 1； the values `TRUE` and `FALSE` are merely aliases for `1` and `0`. | BOOLEAN  | BOOLEAN          |

[MySQL-(M,D) ]:https://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html

`FLOAT(7,4)`  looks like `-999.9999`,so if you insert `999.00009` into a `FLOAT(7,4)` column, the approximate result is `999.0001`.

[MySQL-INT(4) ZEROFILL]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-attributes.html

the optional (nonstandard) `ZEROFILL` attribute, the default padding of spaces is replaced with zeros. For example, for a column declared as [`INT(4) ZEROFILL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), a value of `5` is retrieved as `0005`.

-   [`BOOL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), [`BOOLEAN`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html) These types are synonyms for [`TINYINT(1)`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html). A value of zero is considered false. Nonzero values are considered true; [REFER TO](https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html)





### data_time_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html

| MySQL Data      | Range                                                        | TCM Type  | PgSQL     |
| --------------- | ------------------------------------------------------------ | --------- | --------- |
| DATE            | 1000-01-01 ~ 9999-12-31                                      | DATE      | DATE      |
| TIME [fsp]      | -838:59:59.000000 ~ 838:59:59.000000                         | TIME      | INTERVAL  |
| DATETIME [fsp]  | 1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999;     | TIMESTAMP | TIMESTAMP |
| TIMESTAMP [fsp] | 1970-01-01 00:00:01.000000 UTC ~ 2038-01-19 03:14:07.999999 UTC | TIMESTAMP | TIMESTAMP |
| YEAR [(4)]      | 1901 ~ 2155 Or 0000                                          | INT32     | INT       |

-   An optional *`fsp`* value in the range from 0 to 6 may be given to specify fractional seconds precision. A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.



### ==string_data_types==

https://dev.mysql.com/doc/refman/5.7/en/string-types.html

| MySQL Data        | Range                                                        | TCM Type | PgSQL |
| ----------------- | ------------------------------------------------------------ | -------- | ----- |
| CHAR [M]          | 0 <= M <= 255;default 1;characters                           | STRING   | TEXT  |
| VARCHAR [M]       | 0 <= M <= 65535;characters                                   | STRING   | TEXT  |
| ==BINARY [M]==    | 0 <= M <= 255;default 1; stores binary byte                  | BYTES    | BYTEA |
| ==VARBINARY [M]== | 0 <= M <= 65535; stores binary byte                          | BYTES    | BYTEA |
| ==BLOB [M]==      | 0 <= M <= 65535 （2^16^-1）bytes                             | BYTES    | BYTEA |
| TEXT [M]          | 0 <= M <= 65535 （2^16^-1）characters                        | STRING   | TEXT  |
| ==TINYBLOB==      | maximum length of 255 (2^8^ − 1) bytes                       | BYTES    | BYTEA |
| TINYTEXT          | maximum length of 255 (2^8^ − 1) characters                  | STRING   | TEXT  |
| ==MEDIUMBLOB==    | maximum length of 16,777,215（2^24^-1）bytes                 | BYTES    | BYTEA |
| MEDIUMTEXT        | maximum length of 16,777,215（2^24^-1） characters           | STRING   | TEXT  |
| ==LONGBLOB==      | maximum length of 4,294,967,295（2^32^-1） or 4GB bytes      | BYTES    | BYTEA |
| LONGTEXT          | maximum length of 4,294,967,295（2^32^-1） or 4GB characters | STRING   | TEXT  |
| ENUM              | maximum of 3000 distinct elements,                           | STRING   | TEXT  |
| SET               | maximum of 3000 distinct elements,                           | STRING   | TEXT  |

-   VARCHAR 与 VARBINARY  的存储长度是动态的 [参考](https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html) 与 [参考](https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html#innodb-row-format-compact)，受当前数据库存储引擎不同以及数据库行大小限制的影响，为了避免创建表出错，Source 端不为MySQL时，VARCHAR (0)以及VARBINARY   (0) 都改为 (8192)。
-   ==BINARY==、==VARBINARY==、==BLOB==、==TINYBLOB==、==MEDIUMBLOB==、==LONGBLOB== 导出为十六进制 ，写入为PgSQL的BYTEA类型。
    -   即 mysql 的 `b'0'` 导出为 ’0x30‘，写入到 PgSQL 为四个字节，’0‘、’x‘、’3‘、’0‘ 各占一个字节。





### spatial_data_types

https://dev.mysql.com/doc/refman/5.7/en/spatial-types.html

| MySQL Data | TCM Type | PgSQL |
| ---------- | -------- | ----- |
|     GEOMETRY       | TEXT | TEXT |
|     POINT       | TEXT | TEXT |
|     LINESTRING       | TEXT | TEXT |
|     POLYGON       | TEXT | TEXT |
|     MULTIPOINT       | TEXT | TEXT |
|     MULTILINESTRING       | TEXT | TEXT |
|     MULTIPOLYGON       | TEXT | TEXT |
|     GEOMETRYCOLLECTION       | TEXT | TEXT |

-   由于不同数据库对几何数据的支持不同，因此这儿统一转换为长字符串。



### json_data_type

https://dev.mysql.com/doc/refman/5.7/en/json.html

| MySQL Data | TCM Type | PgSQL |
| ---------- | -------- | ----- |
| JSON       | TEXT     | TEXT  |

-   The space required to store a `JSON` document is roughly the same as for [`LONGBLOB`](https://dev.mysql.com/doc/refman/5.7/en/blob.html) or [`LONGTEXT`](https://dev.mysql.com/doc/refman/5.7/en/blob.html); see [Section 11.7, “Data Type Storage Requirements”](https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html), for more information. It is important to keep in mind that the size of any JSON document stored in a `JSON` column is limited to the value of the [`max_allowed_packet`](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet) system variable. (When the server is manipulating a JSON value internally in memory, it can be larger than this; the limit applies when the server stores it.)

    

