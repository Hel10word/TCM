## Schema 配置参考



### 示例

```json
{
    "tableName":"schemaTable",
    "primaryKeys":[
        "col_tinyint",
        "col_smallint"
    ],
    "columns":[
        {
            "columnName":"col_varchar",
            "characterMaximumPosition":255,
            "mappingDataType":"STRING",
            "nullable":false
        },
        {
            "columnName":"col_decimal",
            "numericPrecision":65,
            "numericScale":30,
            "mappingDataType":"DECIMAL",
            "nullable":true
        },
        {
            "columnName":"col_time_stamp",
            "datetimePrecision":6,
            "mappingDataType":"TIMESTAMP",
            "nullable":true
        }
    ]
}
```



### Schema 配置可选项

-   **tableName**

>   Schema 表的表名
>
>   默认值 : N/A (该项必选)
>
>   参考值 : schemaTable

---

-   **primaryKeys**

>   Primary Keys 是一个数组，里面填写了主键字段，如果是复合主键请按照实际顺序填写
>
>   默认值 : N/A (该项可选)
>
>   参考值 : ["col_tinyint","col_smallint"]

---

-   **columns**

    >   columns 是一个数组，每一个元素也是 JSON 对象，里面可以填写每个列的具体信息与定义，请按照列的实际顺序填写

    -   columnName

        >   Column 名称，该名称也就是 Clone 端生成表的列名
        >
        >   默认值 : N/A (该项必选)
        >
        >   参考值 : col_varchar

        ---

    -   mappingDataType

        >   Column 字段所对应的类型，具体类型请参考 **Mapping Data Type**
        >
        >   默认值 : N/A (该项必选)
        >
        >   参考值 : STRING

        ---

    -   characterMaximumPosition

        >   String 类型可保留的长度
        >
        >   默认值 : N/A (该项可选)
        >
        >   参考值 : 255

        ---

    -   numericPrecision

        >   Decima 类型数字的精度
        >
        >   默认值 : N/A (该项可选)
        >
        >   参考值 : 18

        ---

    -   numericScale

        >   Decima 类型小数点后可保留的长度
        >
        >   默认值 : N/A (该项可选)
        >
        >   参考值 : 2

        ---

    -   datetimePrecision

        >   时间类型的精度
        >
        >   默认值 : N/A (该项可选)
        >
        >   参考值 : 6

        ---

    -   isNullable

        >   列的值可以为 null ，该配置主要在是Clone端生成表的时,对应 Column 是否添加 ` not NULL`
        >
        >   默认值 : true (该项可选)

        ---



### Mapping Data Type

| TCM Type      | 类型描述与范围                                               | 默认 MySQL Type                                              | 默认 PostgreSQL Type           | 默认 SQL Server Type |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------ | -------------------- |
| **INT8**      | Declares 1-byte integer;  `-128` ~ `127`                     | TINYINT                                                      | SMALLINT                       | TINYINT              |
| **INT16**     | Declares 2-byte integer;  `-32,768` ~ `32,767`               | SMALLINT                                                     | SMALLINT                       | SMALLINT             |
| **INT32**     | Declares 3-byte integer;  `-2,147,483,648` ~ `2,147,483,647` | INTEGER                                                      | INT                            | INT                  |
| **INT64**     | Declares 8-byte integer;  `-9,223,372,036,854,775,808` ~ `9,223,372,036,854,775,807` | BIGINT                                                       | BIGINT                         | BIGINT               |
| **FLOAT32**   | Declares 4-byte single-precision float;                      | FLOAT                                                        | REAL                           | REAL                 |
| **FLOAT64**   | Declares 8-byte double-precision float;                      | DOUBLE [(M[,D])]                                             | DOUBLE PRECISION               | FLOAT                |
| **BOOLEAN**   | A variable of this type can have values `true` and `false` or `1` and `0` or `t` and `f`  ； | TINYINT (1)                                                  | BOOLEAN                        | BIT                  |
| **STRING**    | A string type with length ；                                 | VARCHAR [(M)]                                                | VARCHAR [(n)]                  | NVARCHAR             |
| **DECIMAL**   | Decimal data type；                                          | DECIMAL [(M[,D])]                                            | DECIMAL [(precision[,scale])]  | DECIMAL              |
| **DATE**      | Base with `yyyy-MM-dd` format ; `0000-01-01` ~ `9999-12-31`  | DATE                                                         | DATE                           | DATE                 |
| **TIME**      | Base with `HH:mm:ss.SSSSSS` format ;  `-999:59:59.000000` ~ `999:59:59.000000` | TIME [(fsp)]                                                 | TIME [(p)] with time zone      | TIME                 |
| **TIMESTAMP** | Base with `yyyy-MM-dd HH:mm:ss.SSSSSS +- HH:mm` format ; `0000-01-01 000:00:00.000001 + 00:00` ~ `9999-12-31 999:59:59.000000 + 00:00` | TIMESTAMP [(fsp)] DEFAULT CURRENT_TIMESTAMP [(fsp)] ON UPDATE CURRENT_TIMESTAMP[(fsp)] | TIMESTAMP [(p)] with time zone | DATETIMEOFFSET       |
| **TEXT**      | A max-length string type；                                   | LONGTEXT                                                     | TEXT                           | NTEXT                |



### 注意事项

**primaryKeys**、**columns** 这两个字段是 JSON 数组类型，其内部元素需要按照实际顺序填写。