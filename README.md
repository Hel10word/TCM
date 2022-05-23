## CTM-Clone Table Manage

CTM is a tool for initialization table between different datasources . Now supports the following types of databases:

-   MySQL
-   POSTGRES
-   Hudi



## Example

### Configurations

-   **The configuration file uses properties format**

1.   Source Databases  **(Required)**

    ```properties
    sourceDatabaseName=test_db
    # mysql、postgresql
    sourceDataType=mysql
    sourceHost=192.168.120.68
    sourcePort=3306
    sourceUser=root
    sourcePassword=MyNewPass4!
    sourceTable=lineitem_test
    
    #sourceDatabaseName=test_db
    ## mysql、postgresql
    #sourceDataType=postgresql
    #sourceHost=192.168.120.66
    #sourcePort=5432
    #sourceUser=rapids
    #sourcePassword=
    #sourceTable=lineitem_test
    ```

2.  Clone Databases  **(Required)**

    ```properties
    cloneDatabaseName=test_cdc_hudi
    # mysql、postgresql、util
    cloneDataType=hudi
    cloneHost=192.168.120.67
    clonePort=10000
    cloneUser=rapids
    clonePassword=rapids
    cloneTable=lineitem_demo
    
    #cloneDatabaseName=test_db
    #cloneDataType=mysql
    #cloneHost=192.168.30.148
    #clonePort=3306
    #cloneUser=root
    #clonePassword=root
    #cloneTable=data_time_types_mysql_clone
    ```

3.  TCM Configs

    ```properties
    # delete cache file
    # default: true (Optional)
    deleteCache=true
    # During the running process, the path of some temporary files is generated.
    # default: './TCM-TempData/'  (Optional)
    tempDirectory=./TCM-Temp
    # CSV fields delimiter
    # default: '|' (Optional)
    delimiter=,
    # output the information of each step.
    # default: 'false' (Optional)
    debug=false
    ```

    

4.  if you **Clone Data Type is "Hudi"**,need the following information.

    ```properties
    # CSV save to HDFS Path 
    # default: N/A (Required)
    hdfs.source.data.path=hdfs:///HudiTest/
    # util table data save to HDFS Path 
    # default: N/A (Required)
    hdfs.clone.data.path=hdfs://192.168.120.55:9000/HudiTest/demo_Hudi
    # Record key field. Value to be used as the recordKey component of HoodieKey.
    # default: N/A (Required)
    primary.key=id
    # partition path field. Value to be used at the partitionPath component of HoodieKey.
    # default: _hoodie_date (Optional)
    partition.key=ts
    # The table type for the underlying data, for this write.
    # default: 'MERGE_ON_READ' (Required)
    hudi.table.type=COPY_ON_WRITE
    ```

4.  Advanced Configure 

    ```properties
    # Whether to generate  the sql statement of the Source table, and the sql file is saved in the 'tempDirectory'  
    # default: falase (Optional)
    getSourceTableSQL=true
    # Whether to generate  the sql statement of the Clone table, and the sql file is saved in the 'tempDirectory'  
    # default: falase (Optional)
    getCloneTableSQL=true
    # Whether to create a clone table, if not, you need to specify an existing table according to 'cloneTable'. 
    # default: true (Optional)
    createTableInClone=false
    # Whether to execute Export scripts
    # default: true (Optional)
    executeExportScript=false
    # Whether to execute Import scripts
# default: true (Optional)
    executeImportScript=false
    # CSV filename required during import and export 
    # default: Export_'from_DatasourceType'_'TableName'.csv (Optional)
    # csvFileName=""
    ```
    
    

-   Run the following java command to synchronize table data using TCM.

    ```sh
    java -jar TableCloneManage-1-jar-with-dependencies.jar ./test.properties 2>&1 > ./allLog.out
    ```



## Attention

**MySQL** Table Data Export and Load depend on MySQL-Client and MySQL-Shell.

-   your machine has `mysql` and `mysqlsh` command permission.



**PgSQL** Table Data Export and Load depend on PgSQL-Client.

-   your machine has `psql` command permission.



**Hudi** Table Data Load depend on HDFS and Spark.

-   your machine has `spark` and `hdfs` command permission.
-   you spark have  `hudi-spark-bundle_2.xx-x.x.x.jar` environment.





-   MySQL Mapping TCM design by [Mapping in Relation](https://debezium.io/documentation/reference/1.0/connectors/mysql.html#how-the-mysql-connector-maps-data-types_cdc) or view [MysqlMappingTool.java](src/main/java/com/boraydata/tcm\mapping/MysqlMappingTool.java)
-   PgSQL Mapping TCM design by [Mapping in Relation](https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types) or view [PgsqlMappingTool.java](src/main/java/com/boraydata/cdc/tcm/mapping/PgsqlMappingTool.java)

-   TCM Data Type view [TableCloneManageType.java](src/main/java/com/boraydata/cdc/tcm/core/TableCloneManageType.java)


-   TCM Databases Type view [DataTypeMapping.java](src/main/java/com/boraydata/cdc/tcm/core/DataSourceType.java)

    

## Performance

view to [TCM-Performance](OtherInformation/Data Fabric CDC Init (DB-DB) 性能测试结果.xlsx)



## Other

-   The specific execution method can refer to [TableCloneManage.java](src/main/java/com/boraydata/cdc/tcm/DoIt.java)

