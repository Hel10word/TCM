CDC 120.66 root/rdpadmin rapids/rdpuser

HDFS

http://192.168.120.66:50070/dfshealth.html#tab-overview

Hadoop

http://192.168.120.66:8088/cluster

Spark

http://192.168.120.66:8080/

http://192.168.120.66:4040/jobs/



清理 ha节点的 内容

/home/rapids/cdc/hadoop/tmp/nm-local-dir





查看当前任务 

yarn application -list

yarn application -kill <appId>





连接 Hive  

```sql
beeline -u jdbc:hive2://192.168.120.67:10000/test_cdc_hudi -n '' -p '' -e ""
```





CDC 相关的配置文件在 120.66:/etc/rapids/cdc

CDC root/rdpadmin   rapids/rdpuser

psql -h 192.168.120.66 -p 5432 -U root tdb （密码123456）

mysql -h 192.168.120.68 -P 3306 -u root -pMyNewPass4!

66上那个mysql root密码rapids





DB-Hudi

-   [x] db source导出的过程中，使用函数的方式case不支持的字段类型，譬如boolean的0/1 mapping spark的True/False，导出到csv中，使用hudi脚本导入hudi，这个你只需要mapping前面的source数据，不需要熟悉hudi，使用我的工具就可以了，还有查看Spark支持的csv的字符串格式(boolean, timestamp)
-   [x] 保留之前说过的staging方式，使用db-spark的方式mapping，但需要解决spark-hudi的问题，这个今天在会议上Robert说让你去调研这个问题，作为你q4工作的一部分，具体我们遇到的问题是，spark中建表并不能同步到hudi api建的表中，你需要调研如何在导入一张hudi外部的table进hudi table



1.  先将 CSV 文件上传到 HDFS 中。

    ```sh
    hdfs dfs -put ./TCM-Temp/MYSQL_to_HUDI_lineitem_test_mysql.csv hdfs://cdc/tmp/
    
    hdfs dfs -find hdfs://cdc/tmp/MYSQL_to_HUDI_lineitem_test_mysql.csv
    
    hdfs dfs -rm hdfs://cdc/tmp/MYSQL_to_HUDI_lineitem_test_mysql.csv
    ```

    

2.  调用 相应的工具。

-   调用前 请删掉 HDFS 上的 `hive.hdfs.path=hdfs://cdc/tmp/test1_hudi` 文件，里面是存放 Tools 生成的缓存数据文件。
-   请删掉原本的 *.avsc 文件，这是 Tools 生成数据表的记录文件。
-   put CSV 文件到 HDFS 上时，请删原有的 CSV 文件。





[Spark-Shell](https://spark.apache.org/docs/latest/quick-start.html)   [Spark-Shell Command](https://spark.apache.org/docs/3.2.0/submitting-applications.html)

[Hudi](https://hudi.apache.org/docs/quick-start-guide/)



一个巨坑的 地方 Spark 解析 CSV 如果需要用到转义字符，那么需要将字段括起来，也就是只有结合 quote 属性才能 使用 escape。

https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html

经过测试后，发现 **Spark** 所能支持的 CSV 文件，例如

|           |      |
| --------- | ---- |
| delimiter | ，   |
| escape    | 、   |
| quote     | ”    |

1.  根据 CSV 解析到的 元数据

-   `"\,"` => `\,`

-   `"\""` => `"`

-   `"\\` => `\`

2.  根据 元数据 生成的 CSV 数据

-   `,,,` => `",,,"`

-   `\,` => `"\,"`


```scala
val df = spark.read.option("delimiter", ",").option("escape", "\\").option("quote", "\"").csv(path)
df.show()
```

每列数据，会使用 quote 的内容包裹起来，若数据中包含 **escape** 或 **quote** 的数据，则需要在前面加上 escape 来转义。

同样的 **PgSQL** 也会使用，这样的语句导出数据。

```sql
psql postgres://root:123456@192.168.120.66/test_db -c "\copy (select * from demo) to './demo_pgsql.csv' with DELIMITER ',' csv quote '\"' escape '\\' force quote *;"
```

**MySQL** 也能够导出

```sql
mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "select * from demo into outfile '/usr/local/download/demo_mysql.csv' fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n'";
```



-   但是使用 MySQL 无法将数据导出到其他 MySQL 客户端，但是使用如下 MySQL-Shell 会出现一些小问题。

```sql
mysqlsh -h192.168.30.148 -P3306 -uroot -proot --database test_db -e "util.exportTable('demo','./demo_mysql.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:',',fieldsOptionallyEnclosed:true,fieldsEnclosedBy:'\"',fieldsEscapedBy:'\\\'})"
```

​    使用这样的语句，可以将数据导出到我们的客户端，但是MySQL-Shell 对 CSV 的格式有些许不一样，被 quote 的内容包裹起来的数据中，包含 **delimiter** 的内容也会被加上 escape 来转义，因此会出现如下情况：

1.  根据 CSV 解析到的 元数据

-   `"\,"` => `,`

-   `"\""` => `"`

-   `"\\` => `\`

2.  根据 元数据 生成的 CSV 数据

-   `,,,` => `"\,\,\,"`

-   `\,` => `"\\\,"`
