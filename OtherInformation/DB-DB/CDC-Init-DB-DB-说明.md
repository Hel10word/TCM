## Table Clone Manager



>   下面列出了 Table Clone Manager 工具，在进行各个数据源之间 表字段的 Mapping 关系。



| TCM Type      | Description or Range                                         | Default MySQL Type                                           | Default PostgreSQL Type        | Default SQL Server Type |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------ | ----------------------- |
| **INT8**      | Declares 1-byte integer;  `-128` ~ `127`                     | TINYINT                                                      | SMALLINT                       | TINYINT                 |
| **INT16**     | Declares 2-byte integer;  `-32,768` ~ `32,767`               | SMALLINT                                                     | SMALLINT                       | SMALLINT                |
| **INT32**     | Declares 3-byte integer;  `-2,147,483,648` ~ `2,147,483,647` | INTEGER                                                      | INT                            | INT                     |
| **INT64**     | Declares 8-byte integer;  `-9,223,372,036,854,775,808` ~ `9,223,372,036,854,775,807` | BIGINT                                                       | BIGINT                         | BIGINT                  |
| **FLOAT32**   | Declares 4-byte single-precision float;                      | FLOAT                                                        | REAL                           | REAL                    |
| **FLOAT64**   | Declares 8-byte double-precision float;                      | DOUBLE [(M[,D])]                                             | DOUBLE PRECISION               | FLOAT                   |
| **BOOLEAN**   | A variable of this type can have values `true` and `false` or `1` and `0` or `t` and `f`  ； | TINYINT (1)                                                  | BOOLEAN                        | BIT                     |
| **STRING**    | A string type with length ；                                 | VARCHAR [(M)]                                                | VARCHAR [(n)]                  | NVARCHAR                |
| **BYTES**     | Binary data type；                                           | VARBINARY [(M)]                                              | BYTEA                          | NVARBINARY              |
| **DECIMAL**   | Decimal data type；                                          | DECIMAL [(M[,D])]                                            | DECIMAL [(precision[,scale])]  | DECIMAL                 |
| **DATE**      | Base with `yyyy-MM-dd` format ; `0000-01-01` ~ `9999-12-31`  | DATE                                                         | DATE                           | DATE                    |
| **TIME**      | Base with `HH:mm:ss.SSSSSS` format ;  `-999:59:59.000000` ~ `999:59:59.000000` | TIME [(fsp)]                                                 | TIME [(p)] with time zone      | TIME                    |
| **TIMESTAMP** | Base with `yyyy-MM-dd HH:mm:ss.SSSSSS +- HH:mm` format ; `0000-01-01 000:00:00.000001 + 00:00` ~ `9999-12-31 999:59:59.000000 + 00:00` | TIMESTAMP [(fsp)] DEFAULT CURRENT_TIMESTAMP [(fsp)] ON UPDATE CURRENT_TIMESTAMP[(fsp)] | TIMESTAMP [(p)] with time zone | DATETIMEOFFSET          |
| **TEXT**      | A max-length string type；                                   | LONGTEXT                                                     | TEXT                           | NTEXT                   |



-   对于 **FLOAT32**、**FLOAT64**、**STRING**、**DECIMAL** 、**DATE**、**TIME**、**TIMESTAMP** 等数据类型，由于各个数据库的设计与实现不同，例如 PostgreSQL 的 DECIMAL 规定 [1 <= precision <= 1000 , 0 <= scale <= precision](https://www.postgresql.org/docs/13/datatype-numeric.html#DATATYPE-NUMERIC-TABLE)，而 MySQL 的 DECIMAL 规定 [0 <= M <= 65 , 0 <= D <= 30](https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html)，SQL Server 的 DECIMAL 规定 [0 <= p <= 38 , 0 <= s <= p](https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-linux-ver15)，因此 Table Clone Manager 在进行字段类型 Mapping 时，会最大程度满足该数据类型的规定。即 PostgreSQL decimal(100,50) Mapping 到 MySQL 会变成 decimal(65,30)，因此 PostgreSQL 端表数据同步到 MySQL，在这种情况下会出现 **部分数据精度丢失等** 问题。
-   而对于 **BOOLEAN** 数据类型，例如 PostgreSQL  可以将 [ trur、yes、on、1](https://www.postgresql.org/docs/13/datatype-boolean.html) 等都识别为 TRUE，并使用 `t` 表示，FALSE 同理，而 MySQL 只能将 [非 0 的整数](https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html) 识别为 TRUE，并使用 `1` 表示，而将 0 识别为 FALSE。针对这种类型的问题，Table Clone Table 在进行表数据同步时，会创建临时表 来进行数据的转换，使其能够正确的进行同步。






##  MySQL



[refer to]:https://dev.mysql.com/doc/refman/5.7/en/data-types.html

### numeric_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/numeric-types.html

| MySQL Data           | Description or Range                                         | TCM Type |
| -------------------- | ------------------------------------------------------------ | -------- |
| **TINYINT [(M)]**    | Storage of 1-Byte ; 1 <= M <= 225 ; `M` is specifies an with a display width of **M** digits ; M default 4 ; | INT8     |
| **SMALLINT [(M)]**   | Storage of 2-Byte ; `-32768` ~ `32767` ; `M` is specifies an with a display width of **M** digits ; M default 6 ; | INT16    |
| **MEDIUMINT [(M)]**  | Storage of 3-Byte ; `-8388608` ~ `8388607` ; `M` is specifies an with a display width of **M** digits ; M default 9 ; | INT32    |
| **INT [(M)]**        | Storage of 4-Byte ; `-2147483648` ~ `2147483647` ; `M` is specifies an with a display width of **M** digits ; M default 11 ; | INT32    |
| **BIGINT [(M)]**     | Storage of 8-Byte ; -2^63^ (`-9223372036854775808`) ~ 2^63^-1(`9223372036854775807`) ; `M` is specifies an with a display width of **M** digits ; M default 20 ; | INT64    |
| **DECIMAL [(M[,D)]** | 1 <= M <= 65 ; default 10 ; 1 <= D <= 30 ; default 0 ;       | DECIMAL  |
| **FLOAT [(p)]**      | 0 <= p <= 24 , results in a 4-byte single-precision **FLOAT** ; 24< p <= 53,results in a 8-byte double-precision **DOUBLE** ; **default FLOAT(12)** ; | FLOAT32  |
| FLOAT [(M(,D)]       | this is a nonstandard syntax, so TCM converts to **FLOAT64** by default ; | FLOAT64  |
| **DOUBLE [(M(,D)]**  | 1<=M<=65 ; default 10 ;  1 <= D <= 30 ; default 0 ; **default DOUBLE(22)** ; | FLOAT64  |
| **BIT [M]**          | Storage of *`M`*-bit values ; 1 <= M <= 64 ; default 1 ;     | BYTES    |
| TINYINT (1)          | `0` and `1`; A value of zero is considered **FALSE** ; Nonzero values are considered **TRUE** ; | BOOLEAN  |

[MySQL (M,D) ]:https://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html

`FLOAT (7,4)`  looks like `-999.9999`,so if you insert `999.00009` into a `FLOAT(7,4)` column, the approximate result is `999.0001`

[MySQL - INT(4) ZEROFILL]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-attributes.html

the optional (nonstandard) `ZEROFILL` attribute, the default padding of spaces is replaced with zeros. For example, for a column declared as [`INT(4) ZEROFILL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), a value of `5` is retrieved as `0005`.

-   `FIXED [(M,D)]`、`DEC`、`NUMERIC [(M,D)]` as a synonym for **DECIMAL**
-   `INTEGER` as a synonym for **INT**
-   `REAL` as a synonym for **DOUBLE**
-   [`BOOL`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html), [`BOOLEAN`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html) These types are synonyms for [`TINYINT(1)`](https://dev.mysql.com/doc/refman/5.7/en/integer-types.html). A value of zero is considered false. Nonzero values are considered true; [REFER TO](https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html)







### data_time_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/date-and-time-types.html

| MySQL Data            | **Description or Range**                                     | "Zero"  Value         | TCM Type  |
| --------------------- | ------------------------------------------------------------ | --------------------- | --------- |
| **DATE**              | Base with `YYYY-MM-DD` format ; 1000-01-01 ~ 9999-12-31      | '0000-00-00'          | DATE      |
| **TIME [(fsp)]**      | Base with `hh:mm:ss[.fraction]` format ; -838:59:59.000000 ~ 838:59:59.000000 ; `fsp` specify fractional seconds precision ; 0 <= fsp <= 6 ; default 0 ; | '00:00:00'            | TIME      |
| **DATETIME [(fsp)]**  | Base with `YYYY-MM-DD hh:mm:ss[.fraction]` format ; 1000-01-01 00:00:00.000000 ~ 9999-12-31 23:59:59.999999 ; `fsp` specify fractional seconds precision ; 0 <= fsp <= 6 ; default 0 ; | '0000-00-00 00:00:00' | TIMESTAMP |
| **TIMESTAMP [(fsp)]** | 1970-01-01 00:00:01.000000 UTC ~ 2038-01-19 03:14:07.999999 UTC ; `fsp` specify fractional seconds precision ; 0 <= fsp <= 6 ; default 0 ; | '0000-00-00 00:00:00' | TIMESTAMP |
| **YEAR [(4)]**        | Base with `YYYY` format ; 1901 ~ 2155 or 0000                | 0000                  | INT32     |

-   An optional *`fsp`* value in the range from 0 to 6 may be given to specify fractional seconds precision. A value of 0 signifies that there is no fractional part. If omitted, the default precision is 0.



### ==string_data_types_mysql==

https://dev.mysql.com/doc/refman/5.7/en/string-types.html

| MySQL Data                         | Description or Range                                         | TCM Type |
| ---------------------------------- | ------------------------------------------------------------ | -------- |
| **CHAR [(M)]**                     | 0 <= M <= 255 ; default 1                                    | STRING   |
| **VARCHAR (M)**                    | 0 <= M <= 65535 ;                                            | STRING   |
| **==BINARY [(M)]==**               | 0 <= M <= 255 ; default 1; default 1                         | BYTES    |
| **==VARBINARY (M)==**              | 0 <= M <= 65535 ; stores binary byte                         | BYTES    |
| **==BLOB [(M)]==**                 | 0 <= M <= 65535 （2^16^-1）bytes ; default 65535             | BYTES    |
| **TEXT [(M)]**                     | 0 <= M <= 65535 （2^16^-1）characters ; default 65535        | TEXT     |
| **==TINYBLOB==**                   | maximum length of 255 (2^8^ − 1) bytes ;                     | BYTES    |
| **TINYTEXT**                       | maximum length of 255 (2^8^ − 1) characters                  | TEXT     |
| **==MEDIUMBLOB==**                 | maximum length of 16,777,215（2^24^-1）bytes                 | BYTES    |
| **MEDIUMTEXT**                     | maximum length of 16,777,215（2^24^-1） characters           | TEXT     |
| **==LONGBLOB==**                   | maximum length of 4,294,967,295（2^32^-1） or 4GB bytes      | BYTES    |
| **LONGTEXT**                       | maximum length of 4,294,967,295（2^32^-1） or 4GB characters | TEXT     |
| **ENUM ('value1', 'value2', ...)** | maximum of 3000 distinct elements                            | TEXT     |
| **SET ('value1', 'value2', ...)**  | maximum of 3000 distinct elements                            | TEXT     |

-   VARCHAR 与 VARBINARY  的存储长度是动态的 [参考](https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html) 与 [参考](https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html#innodb-row-format-compact)，受当前数据库存储引擎不同以及数据库行大小限制的影响，为了避免创建表出错，Source 端不为MySQL时，STRING (主要对应 mysql 的 VARCHAR ) 类型以及 BYTES (主要对应 mysql 的 VARBINARY )  类型的宽度范围限定为 0 <= M <= 8192 。
-   ==BINARY==、==VARBINARY==、==BLOB==、==TINYBLOB==、==MEDIUMBLOB==、==LONGBLOB== 导出为十六进制 ，写入为PostgreSQL的BYTEA类型。
    -   即 mysql 的 `b'0'` 导出为 ’0x30‘，写入到 PostgreSQL 为四个字节，’0‘、’x‘、’3‘、’0‘ 各占一个字节。





### spatial_data_types_mysql

https://dev.mysql.com/doc/refman/5.7/en/spatial-types.html

| MySQL Data | TCM Type |
| ---------- | -------- |
|     GEOMETRY       | TEXT |
|     POINT       | TEXT |
|     LINESTRING       | TEXT |
|     POLYGON       | TEXT |
|     MULTIPOINT       | TEXT |
|     MULTILINESTRING       | TEXT |
|     MULTIPOLYGON       | TEXT |
|     GEOMETRYCOLLECTION       | TEXT |

-   由于不同数据库对几何数据的支持不同，因此这儿统一转换为长字符串。



### json_data_type_mysql

https://dev.mysql.com/doc/refman/5.7/en/json.html

| MySQL Data | TCM Type |
| ---------- | -------- |
| JSON       | TEXT     |

-   The space required to store a `JSON` document is roughly the same as for [`LONGBLOB`](https://dev.mysql.com/doc/refman/5.7/en/blob.html) or [`LONGTEXT`](https://dev.mysql.com/doc/refman/5.7/en/blob.html); see [Section 11.7, “Data Type Storage Requirements”](https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html), for more information. It is important to keep in mind that the size of any JSON document stored in a `JSON` column is limited to the value of the [`max_allowed_packet`](https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet) system variable. (When the server is manipulating a JSON value internally in memory, it can be larger than this; the limit applies when the server stores it.)

    






## PostgreSQL 

[refer to]:https://www.postgresql.org/docs/13/datatype.html



### numeric_types_pgsql

https://www.postgresql.org/docs/13/datatype-numeric.html

| PostgreSQL Data Type           | Description or Range                                         | TCM Type |
| ------------------------------ | ------------------------------------------------------------ | -------- |
| smallint                       | Storage of 2-Byte ; `-32768` ~ `32767`                       | INT16    |
| integer                        | Storage of 4-Byte ; `-2147483648` ~ `2147483647`             | INT32    |
| bigint                         | Storage of 8-Byte ; -2^63^ (`-9223372036854775808`) ~ 2^63^-1(`9223372036854775807`) ; | INT64    |
| decimal [(precision[, scale])] | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point ; 1 <= precision < 1000 ; 0 <= scale <= precision ; default **null** | DECIMAL  |
| numeric [(precision[, scale])] | up to 131072 digits before the decimal point; up to 16383 digits after the decimal point ; 1 <= precision < 1000 ; 0 <= scale <= precision ; default **null** | DECIMAL  |
| real                           | Storage of 4-Byte ; 6 decimal digits precision ; 1E-37 ~ 1E+37 | FLOAT32  |
| double precision               | Storage of 8-Byte ; 15 decimal digits precision ; 1E-307 ~ 1E+308 | FLOAT64  |
| smallserial                    | Storage of 2-Byte ; `1` ~ `32767`                            | INT16    |
| serial                         | Storage of 4-Byte ; `1` ~ `2147483647`                       | INT32    |
| bigserial                      | Storage of 8-Byte ; `1` ~ 2^63^-1(`9223372036854775807`)     | INT64    |





### monetart_types_pgsql

https://www.postgresql.org/docs/13/datatype-money.html

| PostgreSQL Data | Description or Range                           | TCM Type         |
| --------------- | ---------------------------------------------- | ---------------- |
| money           | -92233720368547758.08 to +92233720368547758.07 | DECIMAL (1000,2) |

-   `-92233720368547758.08` save in money data type is `-$92,233,720,368,547,758.08`，导出时通过类型转换解决。





### character_types_pgsql

https://www.postgresql.org/docs/13/datatype-character.html

| PostgreSQL Data Type    | Description or Range                                         | TCM Type |
| ----------------------- | ------------------------------------------------------------ | -------- |
| character varying [(n)] | variable-length with limit ; 0 <= n <= 10485760 ; default **null** | STRING   |
| varchar [(n)]           | variable-length with limit ; 0 <= n <= 10485760 ; default **null** | STRING   |
| character [(n)]         | fixed-length, blank padded ; 0 <= n <= 10485760 ; default 1  | STRING   |
| char [(n)]              | fixed-length, blank padded ; 0 <= n <= 10485760 ; default 1  | STRING   |
| text                    | variable unlimited length                                    | TEXT     |

-   需要注意 text 类型，在 pgsql 中是可以存放无限长度的,转到 MySQL 中会存在丢弃末尾长度。



### ==binary_types_pgsql==

https://www.postgresql.org/docs/13/datatype-binary.html

| PostgreSQL Data | Description or Range                       | TCM Type |
| --------------- | ------------------------------------------ | -------- |
| ==bytea==       | 1 or 4 bytes plus the actual binary string | BYTES    |

-   由于 PostgreSQL 在对 ==bytea== 字段导出时 默认使用 Hex（十六进制） 进行编码保存，而各个数据库对十六进制的数据导入支持不同，因此不能确保数据的一致性与完整性。







### ==datatime_types_pgsql==

https://www.postgresql.org/docs/13/datatype-datetime.html

| PostgreSQL Data     | Description or Range | TCM Type |
| ------------------------ | ----------------------- | -------- |
| timestamp [(p)] [with time zone] | Storage of 8-Byte ; Base with `yyyy-MM-dd HH:mm:ss.[p]` format ; 4713 BC ~ 294276 AD ; `p` specify fractional seconds precision ; 0 <= p <= 6 ; default 6 | TIMESTAMP |
| timestamp [(p)] with time zone | Storage of 8-Byte; Base with `yyyy-MM-dd HH:mm:ss.[p] +- HH:mm` format  ; 4713 BC ~ 294276 AD ; `p` specify fractional seconds precision ; 0 <= p <= 6 ; default 6 | TIMESTAMP |
| date                     | Storage of 4-Byte ; Base with `yyyy-MM-dd` format ; 4713 BC ~ 5874897 AD | DATE |
| time [(p)] [without time zone] | Storage of 8-Byte ; Base with `HH:mm:ss.[p]` format ; 00:00:00 ~ 24:00:00 ; `p` specify fractional seconds precision ; 0 <= p <= 6 ; default 6 | TIME |
| time [(p)] with time zone | Storage of 12-Byte ; Base with `HH:mm:ss.[p] +- HH:mm` format  ; 00:00:00+1559 ~ 24:00:00-1559 ; `p` specify fractional seconds precision ; 0 <= p <= 6 ; default 6 | TIME |
| ==interval [(p)]== | Storage of 16-Byte ; Base with `+- yyyy +- MM +- dd +- HH +- mm +- ss.[p]` format ; -178000000 years ~ 178000000 years ; `p` specify fractional seconds precision ; 0 <= p <= 6 ; default 6 | STRING |



-    ==interval== 很难找到与之对应的标准存储格式，因此这儿 Mapping 到字符串类型。





### boolean_types_pgsql

https://www.postgresql.org/docs/13/datatype-boolean.html

| PostgreSQL Data | Description or Range                                         | TCM Type |
| --------------- | ------------------------------------------------------------ | -------- |
| boolean         | ('true','yes','on','1',1) => t; ('false','no','off','0',0)=>f | BOOLEAN  |

[MySQL-boolean]:https://dev.mysql.com/doc/refman/5.7/en/numeric-type-syntax.html

-   PostgreSQL 存储 boolean 采用的 t/f ，而MySQL 都识别为0，需要通过转换为 1/0 再插入MySQL；

select *,coalesce((pgboolean::boolean)::int,0) as pgboolean from boolean_types_pgsql；







### ==enumerated_types_pgsql==

https://www.postgresql.org/docs/13/datatype-enum.html

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

| PostgreSQL Data | Description or Range                        | TCM Type |
| --------------- | ------------------------------------------- | -------- |
| point           | Point on a plane                            | TEXT     |
| line            | Infinite line                               | TEXT     |
| lseg            | Finite line segment                         | TEXT     |
| box             | Rectangular box                             | TEXT     |
| path            | Closed path (similar to polygon)；Open path | TEXT     |
| polygon         | Polygon (similar to closed path)            | TEXT     |
| circle          | Circle                                      | TEXT     |

-   由于不同数据库对几何数据的支持不同，因此这儿统一转换为长字符串。






### network_address_types_pgsql

https://www.postgresql.org/docs/13/datatype-net-types.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| cidr            | TEXT     |
| inet            | TEXT     |
| macaddr         | TEXT     |
| macaddr8        | TEXT     |

-   由于 MySQL 中没有与之对应的 字段类型，因此这儿统一转换为长字符串。





### ==bit_string_types_pgsql==

https://www.postgresql.org/docs/13/datatype-bit.html

| PostgreSQL Data   | Description or Range                                         | TCM Type |
| ----------------- | ------------------------------------------------------------ | -------- |
| BIT [(n)]         | without a length specification means unlimited length ; 1 <= n <= 83886080 ; default 1 | BYTES    |
| BIT VARYING [(n)] | without a length specification means unlimited length ; 1 <= n <= 83886080 ; default 1 | BYTES    |

-   由于 PostgreSQL 在对 ==bit== 等字段导出时 默认使用 Hex（十六进制） 进行编码保存，而各个数据库对十六进制的数据导入支持不同，因此不能确保数据的一致性与完整性。





### text_search_types_pgsql

https://www.postgresql.org/docs/13/datatype-textsearch.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| tsvector        | TEXT     |
| tsquery         | TEXT     |

-   由于 MySQL 中没有与之对应的 字段类型，因此这儿统一转换为长字符串。





### uuid_types_pgsql

https://www.postgresql.org/docs/13/datatype-uuid.html

| PostgreSQL Data | TCM Type     |
| --------------- | ------------ |
| uuid            | STRING (255) |

-   默认转为 MySQL : VARCHAR(255)；





### xml_types_pgsql

https://www.postgresql.org/docs/13/datatype-xml.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| xml             | TEXT     |

-   PostgreSQL 中的 XML 类型没有限定长度，因此转为 varchar(255) 可能会溢出，所以将其转换为 LONGTEXT 类型。





### json_types_pgsql

https://www.postgresql.org/docs/13/datatype-json.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| json            | TEXT     |
| jsonb           | TEXT     |

-   由于 PostgreSQL 中的 json 类型也可以存储 text 类型的数据，如果在 MySQL 中使用 varchar (255) 存储会有数据丢失，所以将其转换为 LONGTEXT 类型。 







### array_types_pgsql

https://www.postgresql.org/docs/13/arrays.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| integer[]       | TEXT     |
| text\[][]       | TEXT     |
| integer\[3][3]  | TEXT     |

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

| PostgreSQL Data | Description or Range                   | TCM Type |
| --------------- | -------------------------------------- | -------- |
| int4range       | Range of `integer`                     | TEXT     |
| int8range       | Range of `bigint`                      | TEXT     |
| numrange        | Range of `numeric`                     | TEXT     |
| tsrange         | Range of `timestamp without time zone` | TEXT     |
| tstzrange       | Range of `timestamp with time zone`    | TEXT     |
| daterange       | Range of `date`                        | TEXT     |

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

| PostgreSQL Data | Description or Range         | TCM Type |
| --------------- | ---------------------------- | -------- |
| oid             | numeric object identifier    | TEXT     |
| regclass        | relation name                | TEXT     |
| regcollation    | collation name               | TEXT     |
| regconfig       | text search configuration    | TEXT     |
| regdictionary   | text search dictionary       | TEXT     |
| regnamespace    | namespace name               | TEXT     |
| regoper         | operator name                | TEXT     |
| regoperator     | operator with argument types | TEXT     |
| regproc         | function name                | TEXT     |
| regprocedure    | function with argument types | TEXT     |
| regrole         | role name                    | TEXT     |
| regtype         | data type name               | TEXT     |

-   由于 MySQL 中没有与之对应的类型，因此转为 LONGTEXT 类型存储。





### pg_lsn_types_pgsql

https://www.postgresql.org/docs/13/datatype-pg-lsn.html

| PostgreSQL Data | TCM Type |
| --------------- | -------- |
| pg_lsn          | TEXT     |

[PostgreSQL-pg_lsn]:https://www.postgresql.org/docs/13/datatype-pg-lsn.html

-   由于 pg_lsn 是PostgreSQL的内部数据类型， MySQL 中没有与之对应的类型，因此转为 LONGTEXT 类型存储。









[PostgreSQL-pseudo]:https://www.postgresql.org/docs/13/datatype-pseudo.html

该类型不是用来修饰字段的，是 PostgreSQL 中的伪类型，用来修饰函数，本工具暂不考虑。
















## SQL Server

[refer to]:https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15

###  Exact numerics

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#exact-numerics

| SQL Server Type   | Description or Range                                         | TCM Type       |
| ----------------- | ------------------------------------------------------------ | -------------- |
| bit               | `0` 、`1` 、`NULL`                                           | BOOLEAN        |
| decimal [(p[,s])] | 1 <= p <= 38;default 18 ; 0 <= s <= p ; default 0            | DECIMAL        |
| numeric [(p[,s])] | 1 <= p <= 38;default 18 ; 0 <= s <= p ; default 0            | DECIMAL        |
| tinyint           | 0 ~ 255                                                      | INT8           |
| smallint          | -2^15^ (-32,768) ~ 2^15^-1 (32,767)                          | INT16          |
| int               | -2^31^ (-2,147,483,648) ~ 2^31^-1 (2,147,483,647)            | INT32          |
| bigint            | -2^63^ (-9,223,372,036,854,775,808) ~ 2^63^-1 (9,223,372,036,854,775,807) | INT64          |
| smallmoney        | \- 214,748.3648 ~ 214,748.3647                               | DECIMAL (6,4)  |
| money             | -922,337,203,685,477.5808 ~ 922,337,203,685,477.5807         | DECIMAL (15,4) |

-   bit : The string values TRUE and FALSE can be converted to **bit** values: TRUE is converted to 1 and FALSE is converted to 0 . any nonzero value to 1.



### Approximate numerics 

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#approximate-numerics

| SQL Server Type | Description or Range                                         | TCM Type |
| --------------- | ------------------------------------------------------------ | -------- |
| real            | -3.40E+38 ~ -1.18E-38, 0 and 1.18E-38 ~ 3.40E+38             | FLOAT32  |
| float [(n)]     | 1 <= n <= 53 ; default 53 ; -1.79E+308 ~ -2.23E-308, 0 and 2.23E-308 ~ 1.79E+308 | FLOAT64  |



###  Date and time

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#date-and-time

| SQL Server Type     | Description or Range                                         | TCM Type  |
| ------------------- | ------------------------------------------------------------ | --------- |
| date                | Default format `YYYY-MM-DD` ; 1000-01-01 ~ 9999-12-31; default 1900-01-01 | DATE      |
| time[(n)]           | Default format `hh:mm:ss[.nnnnnnn]` ; 00:00:00.0000000 ~ 23:59:59.9999999; default  00:00:00 | TIME      |
| datetime            | Default format `YYYY-MM-DD hh:mm:ss[.nnn]` ;1753-1-1 00:00:00 ~ 9999-12-31 23:59:59.997; default 1900-01-01 00:00:00 | TIMESTAMP |
| datetime2[(n)]      | Default format `YYYY-MM-DD hh:mm:ss[.nnnnnnn]` ;0001-1-1 00:00:00 ~ 9999-12-31 23:59:59.9999999; default 1900-01-01 00:00:00 | TIMESTAMP |
| datetimeoffset[(n)] | Default format `YYYY-MM-DD hh:mm:ss[.nnnnnnn][{+|-}hh:mm]` ;0001-1-1 00:00:00 ~ 9999-12-31 23:59:59.9999999; default 1900-01-01 00:00:00 00:00 | TIMESTAMP |
| smalldatetime       | Default format `YYYY-MM-DD hh:mm:ss` ;1900-01-01 00:00:00 ~ 2079-06-06 23:59:59; default 1900-01-01 00:00:00 | TIMESTAMP |

>   n : fractional seconds precision ; 0 <= n <= 7 in (time、datetime2、datetimeoffset) type；0 <= n <= 3 in datetime type；

### Character strings

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#character-strings

| SQL Server Type   | Description or Range                                         | TCM Type |
| ----------------- | ------------------------------------------------------------ | -------- |
| char[(n)]         | Fixed-length string data ; 1<= n <= 8000 ; default 1 ; The storage size is *n* bytes | STRING   |
| varchar[(n\|max)] | Variable-size string data ; 1<= n <= 8000 ; default 1 ; the storage size is *n* bytes + 2 bytes ; max storage size is 2^31^-1 bytes (2GB) ; | STRING   |
| text              | Variable-length non-Unicode data ; the storage size may be less than 2^31^-1 （2,147,483,647） bytes (2GB) ; | TEXT     |





### Unicode character strings

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#unicode-character-strings

| SQL Server Type    | Description or Range                                         | TCM Type |
| ------------------ | ------------------------------------------------------------ | -------- |
| nchar[(n)]         | Fixed-length string data ; 1<= n <= 4000 ; default 1 ; The storage size is *n* bytes | STRING   |
| nvarchar[(n\|max)] | Variable-size string data ; 1<= n <= 4000 ; default 1 ; the storage size is *n* bytes + 2 bytes ; max storage size is 2^31^-1 bytes (2GB) ; | STRING   |
| ntext              | Variable-length Unicode data ; the storage size may be less than 2^30^-1 （1,073,741,823） bytes (1GB) ; | TEXT     |







### Binary strings

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#binary-strings

| SQL Server Type     | Description or Range                                         | TCM Type |
| ------------------- | ------------------------------------------------------------ | -------- |
| binary[(n)]         | Fixed-length binary data ; 1<= n <= 8000 ; default 1; The storage size is *n* bytes | BYTE     |
| varbinary[(n\|max)] | Variable-length binary data ; 1<= n <= 8000 ; default 1; The storage size is the actual length of the data entered + 2 bytes ; max storage size is 2^31^-1 bytes (2GB) ; | BYTE     |





### ==Other data types==

https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#other-data-types

| SQL Server Type               | Description or Range | TCM Type |
| ----------------------------- | -------------------- | -------- |
| ==cursor==                    |                      |          |
| ==rowversion==                |                      |          |
| ==hierarchyid==               |                      |          |
| ==uniqueidentifier==          |                      |          |
| ==sql_variant==               |                      |          |
| ==xml==                       |                      |          |
| ==Spatial Types - geometry==  |                      |          |
| ==Spatial Types - geography== |                      |          |
| ==table==                     |                      |          |

