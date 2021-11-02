







## PgSQL 2 MySQL

[refer to]:https://www.postgresql.org/docs/13/datatype.html



### numeric_types_pgsql

https://www.postgresql.org/docs/13/datatype-numeric.html

| PgSQL Data Type  | Range                                                        | TCM Type  | MySQL          |
| ---------------- | ------------------------------------------------------------ | --------- | -------------- |
| smallint         | -32768 to +32767                                             | INT16     | smallint       |
| integer          | -2147483648 to +2147483647                                   | INT32     | int            |
| bigint           | -9223372036854775808 to +9223372036854775807                 | INT64     | bigint         |
| ==decimal==      | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point | 'Decimal' | decimal(65,30) |
| ==numeric==      | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point | 'Decimal' | decimal(65,30) |
| real             | 6 decimal digits precision                                   | FLOAT32   | float          |
| double precision | 15 decimal digits precision                                  | FLOAT64   | double         |
| smallserial      | 1 to 32767                                                   | INT16     | smallint       |
| serial           | 1 to 2147483647                                              | INT32     | int            |
| bigserial        | 1 to 9223372036854775807                                     | INT64     | bigint         |

[mysql-decimal ]:https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html

-   decimal 与 numeric 类型 Mapping 到 MySQL DECIMAL(65,30)，若数据整体长度超过65或小数点后面大于30位会存在精度丢失。

-   由于 PgSQL 的Metadata 只能获取 Data Type，无法获取精度，则统一将其转换为decimal(65,30)



### monetart_types_pgsql

https://www.postgresql.org/docs/13/datatype-money.html

| PgSQL Data | Range                                          | TCM Type | MySQL         |
| ---------- | ---------------------------------------------- | -------- | ------------- |
| money      | -92233720368547758.08 to +92233720368547758.07 | MONEY    | DECIMAL(65,2) |

`-92233720368547758.08` save in money data type is `-$92,233,720,368,547,758.08`，导出时通过类型转换解决。





### character_types_pgsql

https://www.postgresql.org/docs/13/datatype-character.html

| PgSQL Data Type          | Description                | TCM Type | MySQL        |
| ------------------------ | -------------------------- | -------- | ------------ |
| character varying(*`n`*) | variable-length with limit | String   | VARCHAR(256) |
| varchar(*`n`*)           | variable-length with limit | String   | VARCHAR(256) |
| character(*`n`*)         | fixed-length, blank padded | String   | VARCHAR(256) |
| char(*`n`*)              | fixed-length, blank padded | String   | VARCHAR(256) |
| text                     | variable unlimited length  | TEXT     | LONGTEXT     |

-   需要注意 text 类型，在 pgsql 中是可以存放无限长度的,转到 MySQL 中会存在丢弃末尾长度。



### binary_types_pgsql

https://www.postgresql.org/docs/13/datatype-binary.html

| PgSQL Data | Description                   | TCM Type | MySQL           |
| ---------- | ----------------------------- | -------- | --------------- |
| ==bytea==  | variable-length binary string | BYTES    | VARBINARY(1024) |

-   由于 pgsql 在该字段导出时 默认使用 Hex（十六进制） 进行编码保存，因此写入 MySQL 的都是 Hex（十六进制）类型的值。







### datatime_types_pgsql

https://www.postgresql.org/docs/13/datatype-datetime.html

| PgSQL Data               | Range | TCM Type | MySQL |
| ------------------------ | ----------------------- | -------- | ----- |
| timestamp                | 4713 BC ~ 294276 AD     | 'Timestamp' | TIMESTAMP(3) |
| timestamp with time zone | 4713 BC ~ 294276 AD     | 'Timestamp' | TIMESTAMP(3) |
| date                     | 4713 BC ~ 5874897 AD    | DATE | DATE |
| time                     | 00:00:00 ~ 24:00:00               | 'Time' |TIME(3)|
| time with time zone      | 00:00:00+1559 ~ 24:00:00-1559 | 'Time' | TIME(3) |
| interval             | -178000000 years ~ 178000000 years | String | VARCHAR(256) |

[mysql-timestamp]:https://dev.mysql.com/doc/refman/5.7/en/timestamp-initialization.html

-   MySQL 使用 `TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)` 定义 timestamp 类型。





### boolean_types_pgsql

https://www.postgresql.org/docs/13/datatype-boolean.html

| PgSQL Data | True/False in PgSQL                                        | TCM Type | MySQL      |
| ---------- | ---------------------------------------------------------- | -------- | ---------- |
| boolean    | ('true','yes','on','1',1)=>t;('false','no','off','0',0)=>f | BOOLEAN  | TINYINT(4) |

[MySQL-boolean]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html

-   PgSQL 存储 boolean 采用的 t/f ，而MySQL 都识别为0，需要通过转换为 1/0 再插入MySQL；

select *,coalesce((pgboolean::boolean)::int,0) as pgboolean from boolean_types_pgsql；







### ==enumerated_types_pgsql==

https://www.postgresql.org/docs/13/datatype-enum.html

| PgSQL Data |      | TCM Type | MySQL        |
| ---------- | ---- | -------- | ------------ |
| ENUM       |      | STRING   | VARCHAR(256) |

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





### ==geometric_types_pgsql==

https://www.postgresql.org/docs/13/datatype-geometric.html

| PgSQL Data | Description                                 | TCM Type | MySQL |
| ---------- | ------------------------------------------- | -------- | ----- |
| point      | Point on a plane                            |          |       |
| line       | Infinite line                               |          |       |
| lseg       | Finite line segment                         |          |       |
| box        | Rectangular box                             |          |       |
| path       | Closed path (similar to polygon)；Open path |          |       |
| polygon    | Polygon (similar to closed path)            |          |       |
| circle     | Circle                                      |          |       |

-   **暂时不考虑** 几何 类型的数据







### network_address_types_pgsql

https://www.postgresql.org/docs/13/datatype-net-types.html

| PgSQL Data | TCM Type | MySQL        |
| ---------- | -------- | ------------ |
| cidr       | String   | VARCHAR(256) |
| inet       | String   | VARCHAR(256) |
| macaddr    | String   | VARCHAR(256) |
| macaddr8   | String   | VARCHAR(256) |

-   由于 MySQL 中没有与之对应的 字段类型，因此所有的数据都存为 VARCHAR(256)。





### bit_string_types_pgsql

https://www.postgresql.org/docs/13/datatype-bit.html

| PgSQL Data              | TCM Type | MySQL           |
| ----------------------- | -------- | --------------- |
| BIT(1)                  | BYTES    | VARBINARY(1024) |
| ==BIT(n) ;n>1==         | BYTES    | VARBINARY(1024) |
| BIT VARYING(1)          | BYTES    | VARBINARY(1024) |
| ==BIT VARYING(n) ;n>1== | BYTES    | VARBINARY(1024) |

-   在PgSQL中查询官方手册，暂未发现 n 的定义范围。
-   由于Mapping到MySQL中，为  VARBINARY(1024) ，同步长度大于 1024 的**数据会丢失**，而且在MySQL中， VARBINARY(n) 0<=n<=21845。





### text_search_types_pgsql

https://www.postgresql.org/docs/13/datatype-textsearch.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| tsvector   | TEXT     | LONGTEXT |
| tsquery    | TEXT     | LONGTEXT |

-   将其转换为 LONGTEXT 类型。





### uuid_types_pgsql

https://www.postgresql.org/docs/13/datatype-uuid.html

| PgSQL Data | TCM Type | MySQL        |
| ---------- | -------- | ------------ |
| uuid       | String   | VARCHAR(256) |





### xml_types_pgsql

https://www.postgresql.org/docs/13/datatype-xml.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| xml        | TEXT     | LONGTEXT |

-   PgSQL 中的 XML 类型没有限定长度，因此转为 varchar(256) 可能会溢出，所以将其转换为 LONGTEXT 类型。





### json_types_pgsql

https://www.postgresql.org/docs/13/datatype-json.html

| PgSQL Data | TCM Type | MySQL    |
| ---------- | -------- | -------- |
| json       | TEXT     | LONGTEXT |
| jsonb      | TEXT     | LONGTEXT |

-   由于 PgSQL 中的 json 类型也可以存储 text 类型的数据，如果在 MySQL 中使用 varchar (256) 存储会有数据丢失，所以将其转换为 LONGTEXT 类型。 







### array_types_pgsql

https://www.postgresql.org/docs/13/arrays.html

| PgSQL Data     | TCM Type | MySQL    |
| -------------- | -------- | -------- |
| integer[]      | TEXT     | LONGTEXT |
| text\[][]      | TEXT     | LONGTEXT |
| integer\[3][3] | TEXT     | LONGTEXT |

-   ColumnName='pgintefer_[]', DataType='ARRAY', Position=2, dataTypeMapping=null


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

| PgSQL Data | Description                            | TCM Type | MySQL        |
| ---------- | -------------------------------------- | -------- | ------------ |
| int4range  | Range of `integer`                     | String   | VARCHAR(256) |
| int8range  | Range of `bigint`                      | String   | VARCHAR(256) |
| numrange   | Range of `numeric`                     | String   | VARCHAR(256) |
| tsrange    | Range of `timestamp without time zone` | String   | VARCHAR(256) |
| tstzrange  | Range of `timestamp with time zone`    | String   | VARCHAR(256) |
| daterange  | Range of `date`                        | String   | VARCHAR(256) |









### domain_types_pgsql

https://www.postgresql.org/docs/13/domains.html

```sql
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE TABLE domain_types_pgsql (id posint);
INSERT INTO domain_types_pgsql VALUES(1);   -- works
INSERT INTO domain_types_pgsql VALUES(-1);  -- fails
```

-   该类型与其他类型一致，域类型，仅仅是起到限定作用，如上代码，表中的字段类型为 integer 。







### ==object_types_pgsql==

https://www.postgresql.org/docs/13/datatype-oid.html

| PgSQL Data    | Description                  | TCM Type | MySQL |
| ------------- | ---------------------------- | -------- | ----- |
| oid           | numeric object identifier    |          |       |
| regclass      | relation name                |          |       |
| regcollation  | collation name               |          |       |
| regconfig     | text search configuration    |          |       |
| regdictionary | text search dictionary       |          |       |
| regnamespace  | namespace name               |          |       |
| regoper       | operator name                |          |       |
| regoperator   | operator with argument types |          |       |
| regproc       | function name                |          |       |
| regprocedure  | function with argument types |          |       |
| regrole       | role name                    |          |       |
| regtype       | data type name               |          |       |

-   **暂不支持该类型**，debezium 中也没找到该类型的Mapping。





### ==pg_lsn_types_pgsql==

https://www.postgresql.org/docs/13/datatype-pg-lsn.html

| PgSQL Data |      | TCM Type | MySQL |
| ---------- | ---- | -------- | ----- |
| pg_lsn     |      |          |       |

[PgSQL-pg_lsn]:https://www.postgresql.org/docs/13/datatype-pg-lsn.html

-   暂时不支持该数据类型，由于 pg_lsn 是PgSQL的内部数据类型。









[PgSQL-pseudo]:https://www.postgresql.org/docs/13/datatype-pseudo.html

该类型不是用来修饰字段的，是 PgSQL 中的伪类型，用来修饰函数，本工具暂不考虑。













##  MySQ 2 PgSQL



[refer to]:https://dev.mysql.com/doc/refman/5.7/en/data-types.html



### numeric_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/numeric-types.html

| MySQL Data       | Range                                                        | TCM Type | PgSQL            |
| ---------------- | ------------------------------------------------------------ | -------- | ---------------- |
| INTEGER          | -2147483648 ~ 2147483647                                     | INT32    | INT              |
| SMALLINT         | -32768 ~ 32767                                               | INT16    | SMALLINT         |
| DECIMAL[(M,D)]   | 1<=M<=65;default 10;  1<=D<=30;default 0;                    | DECIMAL  | DECIMAL          |
| NUMERIC[(M,D)]   | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| FLOAT(p)         | 0<=p<23,results in a 4-byte single-precision FLOAT；24<=p<53,results in a 8-byte double-precision DOUBLE； | FLOAT64  | DOUBLE PRECISION |
| REAL             | as a synonym for DOUBLE PRECISION                            | FLOAT64  | DOUBLE PRECISION |
| DOUBLE PRECISION | 1<=M<=65;default 10;  1<=D<=30;default 0;                    | FLOAT64  | DOUBLE PRECISION |
| INT              | as a synonym for INTEGER                                     | INT32    | INT              |
| DEC              | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| FIXED[(M,D)]     | as a synonym for DECIMAL                                     | DECIMAL  | DECIMAL          |
| DOUBLE           | as a synonym for DOUBLE PRECISION                            | FLOAT64  | DOUBLE PRECISION |
| ==BIT(1)==       |                                                              | BYTES    | BYTES            |
| ==BIT(M)==       | 1<=M<=64;default 1;                                          | BYTES    | BYTEA            |
| TINYINT          | -128 ~ 127                                                   | INT8     | SMALLINT         |
| MEDIUMINT        | -8388608 ~ 8388607                                           | INT32    | INT              |
| BIGINT           | -2^63^ （-9223372036854775808）~ 2^63^-1（9223372036854775807） | INT64    | BIGINT           |

[MySQL-(M,D) ]:https://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html

`FLOAT(7,4)`  looks like `-999.9999`,so if you insert `999.00009` into a `FLOAT(7,4)` column, the approximate result is `999.0001`.

[MySQL-INT(4) ZEROFILL]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-attributes.html

the optional (nonstandard) `ZEROFILL` attribute, the default padding of spaces is replaced with zeros. For example, for a column declared as [`INT(4) ZEROFILL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), a value of `5` is retrieved as `0005`.

-   [`BOOL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), [`BOOLEAN`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html) These types are synonyms for [`TINYINT(1)`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html). A value of zero is considered false. Nonzero values are considered true:
-   ==MySQL BIT(1)==与==MySQL BIT(M)==导出为十六进制 ，写入为PgSQL的BYTEA类型。





### data_time_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html

| MySQL Data         | Range                                                        | TCM Type  | PgSQL     |
| ------------------ | ------------------------------------------------------------ | --------- | --------- |
| DATE               | 1000-01-01 ~ 9999-12-31                                      | DATE      | DATE      |
| ==TIME [fsp]==     | -838:59:59.000000 ~ 838:59:59.000000                         | INT64     | BIGINT    |
| ==DATETIME [fsp]== | 1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999;     | INT64     | BIGINT    |
| TIMESTAMP [fsp]    | 1970-01-01 00:00:01.000000 UTC ~ 2038-01-19 03:14:07.999999 UTC | TIMESTAMP | TIMESTAMP |
| YEAR [(4)]         | 1901 ~ 2155 Or 0000                                          | INT32     | INT       |

-   An optional *`fsp`* value in the range from 0 to 6 may be given to specify fractional seconds precision. A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.

-   在PgSQL中，Time 类型的时间范围 00:00:00 ~ 24:00:00，超出范围无法存储。

-   DATETIME in PgSQL not Mapping，因为 范围太大了。

-   debezium 中通过 time.precision.mode 的配置信息，将该值转为 微秒值，并存储在 BigInt 中。





### string_data_types

https://dev.mysql.com/doc/refman/5.7/en/string-types.html

| MySQL Data        | Range                                                        | TCM Type | PgSQL |
| ----------------- | ------------------------------------------------------------ | -------- | ----- |
| CHAR [M]          | 0 <= M <= 255                                                | STRING   | TEXT  |
| VARCHAR [M]       | 0 <= M <= 21845 (65535/3)                                    | STRING   | TEXT  |
| ==BINARY [M]==    | 0 <= M <= 255                                                | BYTES    | BYTEA |
| ==VARBINARY [M]== | 0 <= M <= 21845 (65535/3)                                    | BYTES    | BYTEA |
| ==BLOB [M]==      | 0 <= M <= 65535 （2^16^-1）                                  | BYTES    | BYTEA |
| TEXT [M]          | 0 <= M <= 65535 （2^16^-1）                                  | STRING   | TEXT  |
| ==MEDIUMBLOB==    | maximum length of 16,777,215（2^24^-1） characters           | BYTES    | BYTEA |
| MEDIUMTEXT        | maximum length of 16,777,215（2^24^-1） characters           | STRING   | TEXT  |
| ==LONGBLOB==      | maximum length of 4,294,967,295（2^32^-1） or 4GB characters | BYTES    | BYTEA |
| LONGTEXT          | maximum length of 4,294,967,295（2^32^-1） or 4GB characters | STRING   | TEXT  |
| ENUM              | maximum of 3000 distinct elements,                           | STRING   | TEXT  |
| SET               | maximum of 3000 distinct elements,                           | STRING   | TEXT  |

-   除了二进制，与上面 BIT(1) 的问题一样，导出出现格式错误，其余的数据没有问题。







### ==spatial_data_types==

https://dev.mysql.com/doc/refman/5.7/en/spatial-types.html

| MySQL Data | TCM Type | PgSQL |
| ---------- | -------- | ----- |
|     GEOMETRY       | STRUCT | POLYGON |
|     POINT       | STRUCT | POLYGON |
|     LINESTRING       | STRUCT | POLYGON |
|     POLYGON       | STRUCT | POLYGON |
|     MULTIPOINT       | STRUCT | POLYGON |
|     MULTILINESTRING       | STRUCT | POLYGON |
|     MULTIPOLYGON       | STRUCT | POLYGON |
|     GEOMETRYCOLLECTION       | STRUCT | POLYGON |

-   **暂不支持该数据类型**。



### json_data_type

https://dev.mysql.com/doc/refman/5.7/en/json.html

| MySQL Data | TCM Type | PgSQL |
| ---------- | -------- | ----- |
| JSON       | TEXT     | TEXT  |

-   The space required to store a `JSON` document is roughly the same as for [`LONGBLOB`](https://dev.mysql.com/doc/refman/5.7/en/blob.html) or [`LONGTEXT`](https://dev.mysql.com/doc/refman/5.7/en/blob.html); see [Section 11.7, “Data Type Storage Requirements”](https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html), for more information. It is important to keep in mind that the size of any JSON document stored in a `JSON` column is limited to the value of the [`max_allowed_packet`](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet) system variable. (When the server is manipulating a JSON value internally in memory, it can be larger than this; the limit applies when the server stores it.)

    

