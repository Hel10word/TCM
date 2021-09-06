## CTM-Clone Table Manage

CTM is a tool for initialization table between specific datasources . Now supports the following types of databases:

-   MySQL
-   POSTGRES



## Example

1.  Set up Datasource Connection information.

```java
// create connect SourceDB information        
DatabaseConfig.Builder sourceBuilder = new DatabaseConfig.Builder();
DatabaseConfig sourceConfig = sourceBuilder
    .setDatabasename("test_db")
    .setDataSourceType(DataSourceType.MYSQL)
    .setHost("192.168.30.192")
    .setPort("3306")
    .setUsername("root")
    .setPassword("root")
    .create();

// create connect CloneDB information  
DatabaseConfig.Builder cloneBuilder = new DatabaseConfig.Builder();
DatabaseConfig cloneConfig = cloneBuilder
    .setDataSourceType(DataSourceType.POSTGRES)
    .setUrl("jdbc:postgresql://192.168.30.192:5432/test_db")
    .setUsername("postgres")
    .setPassword("")
    .create();
```

2.  use Datasource Connection information and TCMFactory to create TCM

```java
TableCloneManageContext.Builder tcmcBuilder = new TableCloneManageContext.Builder();
TableCloneManageContext tcmContext = tcmcBuilder
    .setSourceConfig(sourceConfig)
    .setCloneConfig(cloneConfig)
    .create();

TableCloneManage tcm = TableCloneManageFactory.createTableCloneManage(tcmContext);
```

3.  use the TCM to initialization table between MySQL and PgSQL

```java
// get Table information by TableName in MySQL(SourceDataBase)
Table sourceTable = tcm.getSourceTable("colume_type");

// get Mapping Table in PgSQL(CloneDataBase) by sourceTable
Table cloneTable = tcm.mappingCloneTable(sourceTable);

// you can set TableName，defautl use 'sourcetable.TableName'
// cloneTable.setTablename("colume_type_copy");

// tcm will create table in PgSQL(CloneDataBase) samme as table "colume_type"
boolean flag = tcm.createTableInCloneDatasource(cloneTable);
if(flag)
	System.out.println("create table Success");
else
	System.out.println("Create table Failure");
```



The complete demo case can be viewed [.\src\main\java\com\boraydata\tcm\Test.java](.\src\branch\master\src\main\java\com\boraydata\tcm\Test.java)



## API

`Class TableCloneManage`



-   Table getSourceTable(String tablename)

>   根据 **表名** 从 SourceDataBase 中获取表的相关信息，并且 设置每个字段映射到 TCM 中的类型，方便后续根据 TCM 类型转换成其他的 数据类型，并将该表返回。



-   Table mappingCloneTable(Table table)

>   会将 **提供的表** 做一次克隆**深拷贝**，然后在克隆后的表上进行修改，设置表中每个列的 DataType 值，会根据每个列的 TCM 类型 和 TCM 的映射关系，然后映射成 CloneDataBase 所能接纳的 DataType，并将修改后的 表返回。



-   boolean createTableInCloneDatasource(Table table)

>   会使用 **提供的表** 在 CloneDataBase 中创建，在 tcm 初始化时创建的 CloneMappingTool ，每个MappingTool 都会实现对应数据库的 建表语法生成，以及一些其他的方法，请查看 MappingTool 接口，然后根据 **提供的表** 生成的建表语句，在CloneDataBase上执行，将执行结果作为 boolean 结果返回。



## Attention

-   MySQL Mapping TCM design by [Mapping in Relation](https://debezium.io/documentation/reference/1.0/connectors/mysql.html#how-the-mysql-connector-maps-data-types_cdc) or view [MysqlMappingTool.java](.\src\branch\master\src\main\java\com\boraydata\tcm\mapping\MysqlMappingTool.java)
-   PgSQL Mapping TCM design by [Mapping in Relation](https://debezium.io/documentation/reference/1.0/connectors/postgresql.html#data-types) or view [PgsqlMappingTool.java](.\src\branch\master\src\main\java\com\boraydata\tcm\mapping\PgsqlMappingTool.java)



-   TCM Mapping Database design by  [Mapping out Relation](https://docs.confluent.io/3.1.1/connect/connect-jdbc/docs/sink_connector.html#auto-creation-and-auto-evoluton) or view [DataTypeMapping.java](.\src\branch\master\src\main\java\com\boraydata\tcm\core\DataTypeMapping.java)



## Other

-   work process or viewed [TableCloneManage.java](.\src\branch\master\src\main\java\com\boraydata\tcm\core\TableCloneManage.java)

    ```java
    (1).use SQL ( by '*.core.DataSourceType' and TableName) query table info and save as Table ( name = 'sourceTable' ).
    
    (2).use '*MappingTool' to mapping 'sourceTable' datatype to TCM datatype,
        and set Table.columns.DataTypeMapping. ( name = 'sourceMappingTable' )
        
    (3).clone 'sourceMappingTable' temp table,set Table.columns.datatype use  '*.core.DataTypeMapping' ( name = cloneTable)
    
    (4).use '*MappingTool' to create cloneTable Create Table SQL and Execute SQL.
    ```

