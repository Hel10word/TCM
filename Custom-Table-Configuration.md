## Custom Table Configure



### Example 

```json
{
    "tableName":"customTable",
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



### Custom Table Configurations Options

-   **tableName**

>   Custom table name
>
>   Default Value : N/A (Required)
>
>   Example  Value : customTable

---

-   **primaryKeys**

>   Primary Keys Is a list of custom table is specified
>
>   Default Value : N/A (Optional)
>
>   Example  Value : ["col_tinyint","col_smallint"]

---

-   **columns**

    >   columns Is a list,It configures the information of all the columns of the custom table

    -   columnName

        >   Column Name,This name is also the column name of clone table
        >
        >   Default Value : N/A (Required)
        >
        >   Example  Value : col_varchar

        ---

    -   mappingDataType

        >   Column data type,optional values please refer to **Mapping Data Type**
        >
        >   Default Value : N/A (Required)
        >
        >   Example  Value : STRING

        ---

    -   characterMaximumPosition

        >   Column character maximum position
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 255

        ---

    -   numericPrecision

        >   Column numeric position
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 18

        ---

    -   numericScale

        >   Column numeric scale
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 2

        ---

    -   datetimePrecision

        >   Column datetime scale
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 6

        ---

    -   isNullable

        >   Column is null able
        >
        >   Default Value : true (Optional)

        ---



### Mapping Data Type

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
| **DECIMAL**   | Decimal data type；                                          | DECIMAL [(M[,D])]                                            | DECIMAL [(precision[,scale])]  | DECIMAL                 |
| **DATE**      | Base with `yyyy-MM-dd` format ; `0000-01-01` ~ `9999-12-31`  | DATE                                                         | DATE                           | DATE                    |
| **TIME**      | Base with `HH:mm:ss.SSSSSS` format ;  `-999:59:59.000000` ~ `999:59:59.000000` | TIME [(fsp)]                                                 | TIME [(p)] with time zone      | TIME                    |
| **TIMESTAMP** | Base with `yyyy-MM-dd HH:mm:ss.SSSSSS +- HH:mm` format ; `0000-01-01 000:00:00.000001 + 00:00` ~ `9999-12-31 999:59:59.000000 + 00:00` | TIMESTAMP [(fsp)] DEFAULT CURRENT_TIMESTAMP [(fsp)] ON UPDATE CURRENT_TIMESTAMP[(fsp)] | TIMESTAMP [(p)] with time zone | DATETIMEOFFSET          |
| **TEXT**      | A max-length string type；                                   | LONGTEXT                                                     | TEXT                           | NTEXT                   |





### Attention

**primaryKeys**、**columns** are all JSON Array Type, please fill in the internal elements in the actual order.