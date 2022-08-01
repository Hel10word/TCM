## CTM-Clone Table Manage

CTM is a tool for initialization table between different Data Source . Now supports the following types of databases:

-   Source Data Source Supports
    -   MySQL
    -   RPDSQL
    -   SQLSERVER
    -   POSTGRES
-   Source Data Source Supports
    -   MySQL
    -   RPDSQL
    -   SQLSERVER
    -   POSTGRES
    -   Hudi



## Example

### Basic Configurations

#### DB-DB

```json
{
  "sourceConfig":{
    "dataSourceEnum":"POSTGRESQL",
    "host":"192.168.30.38",
    "port":"5432",
    "username":"postgres",
    "password":"postgres",
    "databaseName":"test_db",
    "catalog":"test_db",
    "schema":"public"
  },
  "cloneConfig":{
    "dataSourceEnum":"MYSQL",
    "host":"192.168.30.38",
    "port":"3306",
    "username":"root",
    "password":"root",
    "databaseName":"test_db"
  },
  "redis":{
    "host":"192.168.120.66",
    "messageKey":"TestRedisKey",
    "port":"16379"
  },
  "sourceTableNames":["lineitem_test","lineitem_test"],
  "cloneTableNames":["lineitem_test_pg1","lineitem_test_pg2"],
  "createTableInClone":true,
  "createPrimaryKeyInClone":true,
  "executeExportScript":true,
  "executeLoadScript":true,
  "tempDirectory":"/data/cdc_data/fabric-cdc/init/TCM-Temp",
  "deleteCache":false,
  "delimiter":"|",
  "lineSeparate": "\\n",
  "quote": null,
  "escape": null,
  "sendRedis":true
}
```



#### Custom Table - DB

```json
{
  "sourceConfig":{
    "dataSourceEnum":"RPDSQL",
    "host":"192.168.120.69",
    "port":"3306",
    "username":"root",
    "password":"rdpuser",
    "databaseName":"test_db"
  },
  "cloneConfig":{
    "dataSourceEnum":"SQLSERVER",
    "host":"192.168.120.237",
    "port":"1433",
    "username":"sa",
    "password":"Rapids123*",
    "databaseName":"test_db",
    "catalog":"test_db",
    "schema":"dbo"
  },
  "redis":{
    "host":"192.168.120.66",
    "messageKey":"TestRedisKey",
    "port":"16379"
  },
  "sourceTableNames":[null,null],
  "cloneTableNames":["customTable_test_sql1","customTable_test_sql2"],
  "customTables":[{"tableName":"customTable","primaryKeys":["col_tinyint","col_smallint"],"columns":[{"columnName":"col_varchar","characterMaximumPosition":255,"mappingDataType":"STRING","nullable":false},{"columnName":"col_decimal","numericPrecision":65,"numericScale":30,"mappingDataType":"DECIMAL","nullable":true},{"columnName":"col_time_stamp","datetimePrecision":6,"mappingDataType":"TIMESTAMP","nullable":true}]},{"tableName":"customTable","primaryKeys":["col_tinyint","col_smallint"],"columns":[{"columnName":"col_varchar","characterMaximumPosition":255,"mappingDataType":"STRING","nullable":false},{"columnName":"col_decimal","numericPrecision":65,"numericScale":30,"mappingDataType":"DECIMAL","nullable":true},{"columnName":"col_time_stamp","datetimePrecision":6,"mappingDataType":"TIMESTAMP","nullable":true}]}],
  "useCustomTables":true,
  "outSourceTableSQL":true,
  "outCloneTableSQL":true,
  "createTableInClone":true,
  "createPrimaryKeyInClone":true,
  "executeExportScript":true,
  "executeLoadScript":true,
  "tempDirectory":"/data/cdc_data/fabric-cdc/init/TCM-Temp",
  "deleteCache":false,
  "delimiter":"|",
  "lineSeparate": "\\n",
  "sendRedis":true
}
```



#### DB - Hudi

```json
{
  "sourceConfig":{
    "dataSourceEnum":"POSTGRESQL",
    "host":"192.168.30.38",
    "port":"5432",
    "username":"postgres",
    "password":"postgres",
    "databaseName":"test_db",
    "catalog":"test_db",
    "schema":"public"
  },
  "cloneConfig":{
    "dataSourceEnum":"HUDI",
    "host":"192.168.120.67",
    "port":"10000",
    "username":"rapids",
    "password":"rapids",
    "databaseName":"test_db"
  },
  "redis":{
    "host":"192.168.120.66",
    "messageKey":"TestRedisKey",
    "port":"16379"
  },
  "sourceTableNames":["lineitem_test"],
  "cloneTableNames":["lineitem_test_hudi"],
  "hdfsSourceDataDir":"hdfs:///cdc-init/",
  "hdfsCloneDataPath":"hdfs:///cdc-init/test_test_hudi_pg",
  "primaryKey":"l_comment",
  "partitionKey":"_hoodie_date",
  "hoodieTableType":"MERGE_ON_READ",
  "nonPartition":false,
  "multiPartitionKeys":false,
  "sparkCustomCommand":"spark-shell \\\n            --jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \\\n            --driver-class-path $HADOOP_CONF_DIR \\\n            --packages org.apache.spark:spark-avro_2.11:2.4.4 \\\n            --master local[2] \\\n            --deploy-mode client \\\n            --driver-memory 4G \\\n            --executor-memory 4G \\\n            --num-executors 2",
  "csvSaveInHDFS":false,
  "executeExportScript":true,
  "executeLoadScript":true,
  "csvFileName": "to-hudi.csv",
  "tempDirectory":"/data/cdc_data/fabric-cdc/init/TCM-Temp",
  "deleteCache":false,
  "delimiter":"|",
  "lineSeparate": "\n",
  "quote": "\"",
  "escape": "\\",
  "sendRedis":true,
  "debug":false
}
```

### All Configurations Options

-   **sourceConfig** 、 **cloneConfig**

    -   datasourceEnum

        >   Data Source Type 
        >
        >   Default Value : N/A (Required)
        >
        >   Option Value : mysql、postgresql、rpdsql、hudi、sqlserver
        >
        >   Example Value : mysql

    -   host

        >   Default Value : N/A (Required)
        >
        >   Example  Value : 127.0.0.1

    -   port

        >   Default Value : N/A (Required)
        >
        >   Example  Value : 3306

    -   username

        >   Default Value : N/A (Required)
        >
        >   Example  Value : root

    -   password

        >   Default Value : N/A (Required)
        >
        >   Example  Value : password

    -   databaseName

        >   Default Value : N/A (Required)
        >
        >   Example  Value : test_db

    -   catalog

        >   catalog where the table is located
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : def

    -   schema

        >   schema where the table is located
        >
        >   Default Value : N/A (Optional)
        >
        >   Example  Value : test_db

    ---

-   **redis**

    -   host

        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 127.0.0.1

    -   prot

        >   Default Value : N/A (Optional)
        >
        >   Example  Value : 16379

    -   auth

        >   Default Value : N/A (Optional)
        >
        >   Example  Value : admin

    -   messageKey

        >   Default Value : N/A (Optional)
        >
        >   Example  Value : testKey

    -   expirSecound

        >   Redis message expire seconds 
        >
        >   Default Value : 86400 (Optional)
        >
        >   Example  Value : 60

    ---

-   **sourceTableNames**

    >   Source table name Is a list, need to ensure that the length corresponds to **cloneTableNames** one by one.
    >
    >   Default Value : N/A (Required)
    >
    >   Example  Value : ["table-1","table-2","table-1"]

    ---

-   **cloneTableNames**

    >   Clone table name Is a list
    >
    >   Default Value : N/A (Required)
    >
    >   Example  Value : ["table-clone-1","table-clone-2","table-clone-3"]

    ---

-   **customTables**

    >   Clone table name Is a list, need to ensure that the length corresponds to **cloneTableNames** one by one.
    >
    >   Default Value : N/A (Optional)
    >
    >   Example  Value : [{"tableName":"customTable",…},{….},{….}]

    ---

-   **useCustomTables**

    >   Prefer to use Custom Tables information as source table,if true make sure **customTables** not empty 
    >
    >   Default Value : false (Optional)

    ---

-   **outSourceTableSQL**

    >   record the create table statement of the Source table, *.sql file save in **tempDirectory**
    >
    >   Default Value : false (Optional)

    ---

-   **outCloneTableSQL**

    >   record the create table statement of the Clone table, *.sql file save in **tempDirectory**
    >
    >   Default Value : false (Optional)

    ---

-   **createTableInClone**

    >   whether create table in Clone,if false ,need to make sure the **Clone Table** exists
    >
    >   Default Value : true(Optional)

    ---

-   **createPrimaryKeyInClone**

    >   whether create table in Clone witch primary keys
    >
    >   Default Value : true(Optional)

    ---

-   **executeExportScript**

    >   whether execute export csv file script from source.
    >
    >   Default Value : true(Optional)

    ---

-   **executeLoadScript**

    >   whether execute load csv file script to clone.
    >
    >   Default Value : true(Optional)

    ---

-   **csvFileName**

    >   assign export csv file name
    >
    >   Default name format : Export_from_ + sourceType + _ + tableName + .csv (Optional)
    >
    >   Example  Value : Export_Source_Table.csv

    ---

-   **tempDirectory**

    >   generate temp file directory
    >
    >   Default Value : ./TCM-Temp(Optional)

    ---

-   **delimiter**

    >   Separator for each field of CSV file
    >
    >   Default Value : | (Optional)

    ---

-   **lineSeparate**

    >   Separator for each line of CSV file
    >
    >   Default Value : \n (Optional)

    ---

-   **quote**

    >   quoter each field of CSV file
    >
    >   Default Value : N/A (Optional)
    >
    >   Example  Value : "

    ---

-   **escape**

    >   escape delimiter of CSV file
    >
    >   Default Value : N/A (Optional)
    >
    >   Example  Value : \

    ---

-   **sendRedis**

    >   send message to Redis,if true make sure **redis** not empty
    >
    >   Default Value : true(Optional)

    ---

-   **debug**

    >   Whether to output detailed information during the running process
    >
    >   Default Value : false(Optional)

---





**Custom Table Configuration**

view to [Custom Table Configuration](./Custom Table Configuration.md)





**Hudi Configuration**

-   **hdfsSourceDataDir**

    >   CSV file save to HDFS Path,because  load csv to hudi can from local path or HDFS path
    >
    >   Default Value : N/A(Optional)
    >
    >   Example  Value : hdfs://127.0.0.1:9000/csv-path/

    ---

-   **hdfsCloneDataPath**

    >   hudi table data save path in HDFS, please make sure this directory is empty
    >
    >   Default Value : N/A(Required)
    >
    >   Example  Value : hdfs://127.0.0.1:9000/hudi-path/

    ---

-   **primaryKey**

    >   hudi Record key field. Normally the sourceTable primary key or UUID.
    >
    >   Default Value : N/A (Required)
    >
    >   Example  Value : id

    ---

-   **partitionKey**

    >   hudi Partition path field,Value to be used at the patitionPath component of HoodieKey.
    >
    >   Default Value : _hoodie_date (Optional)

    ---

-   **hoodieTableType**

    >   hudi type of table to write
    >
    >   Default Value : MERGE_ON_READ (Required)
    >
    >   Optional Value : COPY_ON_WRITE、MERGE_ON_READ

    ---

-   **nonPartition**

    >   hudi table should or should not be multiPartitioned.whether have hive partition field, if false,default use time field to partition
    >
    >   Default Value : false (Optional)

    ---

-   **multiPartitionKeys**

    >   hudi table should or should not be multiPartitioned,whether to support hive multi-partition field
    >
    >   Default Value : false (Optional)

    ---

-   **sparkCustomCommand**

    >   start spark job command
    >
    >   Default Value : N/A(Required)
    >
    >   Example  Value : spark-shell --driver-class-path $HADOOP_CONF_DIR --master local[2] --deploy-mode client --driver-memory 4G  --executor-memory 4G --num-executors 2

    ---

-   **csvSaveInHDFS**

    >   export csv File, and put in HDFS,can reduce the pressure on the disk during run spark job,if set true ,need set  **hdfsSourceDataDir**
    >
    >   Default Value : false(Optional)

---



-   Run the following java command to synchronize table data using TCM.

    ```sh
    java -jar TableCloneManage-1-jar-with-dependencies.jar ./test.json 2>&1 > ./allLog.out
    ```



## Attention

**MySQL** or **RPDSQL** Table Data Export and Load depend on MySQL-Client and MySQL-Shell.

-   your machine has `mysql` and `mysqlsh` command permission.

**PgSQL** Table Data Export and Load depend on PgSQL-Client.

-   your machine has `psql` command permission.

**Sql Server** Table Data Export and Load depend on PgSQL-Client.

-   your machine has `bcp` command permission.

**Hudi** Table Data Load depend on HDFS and Spark.

-   your machine has `spark` and `hdfs` command permission.
-   you spark have  `hudi-spark-bundle_2.xx-x.x.x.jar` environment.



-   **Complete Process**

    1.  Get table information of source or custom
    2.  Delete clone table by clone table name
    3.  Create mapping table in clone
    4.  Export source table data in temp directory
    5.  Load csv data to clone
    6.  Delete the related files under the temp directory 
    7.  Send message to redis or print log

    >   The files generated in the above process will be in the temp directory 



## Performance

view to [TCM-Performance](./OtherInformation/CDC初始化同步性能测试.md)



## Other

-   The specific execution method can refer to [TableCloneManage.java](src/main/java/com/boraydata/cdc/tcm/TableCloneManagerLauncher.java)
-   The table field mapping relationship please refer to  [CDC Init DB-DB 说明.md](./OtherInformation/DB-DB/CDC Init DB-DB 说明.md)

