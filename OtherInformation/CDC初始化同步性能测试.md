# CDC初始化同步性能测试

## 测试环境

-   **10.11** : MySQL
    
    -   MySQL 8.0.29-0ubuntu0.20.04.3
    -   Username : `root` Password : `rdpadmin`
    
-   **10.11** : PostgreSQL
    -   PostgreSQL 12.11 (Debian 12.11-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    -   Username : `postgres` Password : `123456`
    
-   **120.237** : Sql Server 
    -   Microsoft SQL Server 2017 (RTM-CU28) (KB5008084) - 14.0.3430.2 (X64) Developer Edition (64-bit) on Linux (CentOS Linux 7 (Core)).
    -   Username : `sa` Password : `Rapids123*`
    
-   **120.251**: 测试节点

    -   原生语句测试 与 CDC-init测试均在该节点上进行。

    

## 测试数据

-   `lineitem_sf10` row：59,986,052

-   `lineitem_sf100` row：600,037,830

-   各个数据源的数据表均无主键约束、没有建立索引




## 原生 导入导出方式

>   ​	每次导出数据前，都会先删除本地 CSV 文件，每次导入数据前，都会先清空表内数据。

### PgSQL

#### sf10

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "\COPY lineitem_sf10 TO './test.csv' WITH DELIMITER '|' NULL '' "
```

>   2m16.837 s、2m23.448 s、2m21.867 s

-   清除表数据 

```sh
psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "truncate table lineitem_sf10"
```

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "\COPY lineitem_sf10 FROM './test.csv' WITH DELIMITER '|' NULL '' "
```

>   6m29.699 s、6m37.161 s、6m13.572 s

#### sf100

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "\COPY lineitem_sf100 TO './test.csv' WITH DELIMITER '|' NULL '' "
```

>   22m32.342 s，26m29.776 s，23m37.353 s

-   清除表数据 

```sh
psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "truncate table lineitem_sf100"
```

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time psql postgres://postgres:123456@192.168.10.11:6432/test_db -c "\COPY lineitem_sf100 FROM './test.csv' WITH DELIMITER '|' NULL '' "
```

>   62m38.397 s，63m40.329 s，64m5.760 s      






### MySQL

#### sf10

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time mysqlsh -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db -e "util.exportTable('lineitem_sf10','./test.csv',{fieldsTerminatedBy:'|',linesTerminatedBy:'\n'})"
```

>   3m15.285 s、3m14.603 s、3m19.577 s

-   清空表数据

```sh
mysql -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db -e "truncate table lineitem_sf10"
```

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time mysql -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db --local-infile=ON -e "LOAD DATA LOCAL INFILE './test.csv' INTO TABLE lineitem_sf10 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n'"
```

>   20m48.004 s、20m44.517 s、20m56.376 s

#### sf100

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time mysqlsh -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db -e "util.exportTable('lineitem_sf100','./test.csv',{fieldsTerminatedBy:'|',linesTerminatedBy:'\n'})"
```

>   32m20.708 s、31m50.304 s、30m56.720 s

-   清空表数据

```sh
mysql -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db -e "truncate table lineitem_sf100"
```

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time mysql -h 192.168.10.11 -P 3306 -u root -prdpadmin --database test_db --local-infile=ON -e "LOAD DATA LOCAL INFILE './test.csv' INTO TABLE lineitem_sf100 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n'"
```

>   206m39.345 s、203m15.131 s 、206m47.658 s



### SQL SERVER

#### sf10

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time bcp "select * from dbo.lineitem_sf10" queryout './test.csv' -S 192.168.120.237,1433 -U sa -P Rapids123* -d test_db -c -a 4096 -t '|' -r '\n'
```

>   8m35.642 s、8m53.642 s、8m23.948 s

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time bcp "dbo.lineitem_sf10" in './test.csv' -S 192.168.120.237,1433 -U sa -P Rapids123* -d test_db -c -a 4096 -t '|' -r '\n'
```

>   27m5.914 s、26m4.603 s、28m57.263 s

#### sf100

-   export 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time bcp "select * from dbo.lineitem_sf100" queryout './test.csv' -S 192.168.120.237,1433 -U sa -P Rapids123* -d test_db -c -a 4096 -t '|' -r '\n'
```

>   86m26.710 s、85m26.956 s、84m16.112 s

-   load 远程

```sh
# 在 192.168.120.251 节点上运行该语句
time bcp "dbo.lineitem_sf100" in './test.csv' -S 192.168.120.237,1433 -U sa -P Rapids123* -d test_db -c -a 4096 -t '|' -r '\n'
```

>   353m41.342 s、365m43.537 s、382m38.515 s





## CDC-Init 全量导入导出方式

| Source -> Sink           | Export sf10 | Load sf10 | Export sf100 | Load sf100 |
| ------------------------ | ----------- | --------- | ------------ | ---------- |
| Postgresql -> Postgresql | 137 s       | 383 s     | 1391 s       | 3760 s     |
| Postgresql -> MySQL      | 151 s       | 1073 s    | 1573 s       | 10600 s    |
| Postgresql -> Sql Server | 133 s       | 1546 s    | 1377 s       | 23473 s    |
|                          |             |           |              |            |
| MySQL -> Postgresql      | 184 s       | 376 s     | 1965 s       | 3920 s     |
| MySQL -> MySQL           | 190 s       | 1089 s    | 1936 s       | 10611 s    |
| MySQL -> Sql Server      | 176 s       | 1643 s    | 1974 s       | 23573 s    |
|                          |             |           |              |            |
| Sql Server -> Postgresql | 515 s       | 371 s     | 5078 s       | 3949 s     |
| Sql Server -> MySQL      | 602s        | 1044 s    | 5523 s       | 10395 s    |
| Sql Server -> Sql Server | 521 s       | 1419 s    | 5155 s       | 23484 s    |

>   MySQL Export ：生成 mysqlsh Export Shell 脚本，Java 调用该脚本进行数据的导出。
>
>   MySQL Load ：使用 JDBC 的方式执行 Load Data File 语句进行数据的导入。
>
>   PgSQL Export ：生成 psql Export Shell 脚本，Java 调用该脚本进行数据的导出。
>
>   PgSQL Load ：生成 psql LoadShell 脚本，Java 调用该脚本进行数据的导入。
>
>   Sql Server Export ：生成 bcp Export Shell 脚本，Java 调用该脚本进行数据的导出。
>
>   Sql Server Load ：生成 bcp LoadShell 脚本，Java 调用该脚本进行数据的导入。

## 性能测试结果对比 



### 使用原生语句

>   对每个数据源进行了三次测试，最终结果取 3 次的平均值，

|            | CSV Path | Export sf10 | Load sf10 | Export sf100 | Load sf100 |
| ---------- | -------- | ----------- | --------- | ------------ | ---------- |
| PostgreSQL | 远程     | 2 m 20 s    | 6 m 26 s  | 24 m 13 s    | 63 m 27 s  |
| MySQL      | 远程     | 3 m 16 s    | 20 m 49 s | 31 m 42 s    | 205 m 34 s |
| Sql Server | 远程     | 8 m 37 s    | 27 m 22 s | 85 m 22 s    | 367 m 20 s |



### CDC-Init 

>   通过排列组合三种数据源，使用 CDC-Init 的方式，对每个数据源进行了导入导出测试，结果取平均值。

|            | CSV Path | Export sf10 | Load sf10 | Export sf100 | Load sf100 |
| ---------- | -------- | ----------- | --------- | ------------ | ---------- |
| PostgreSQL | 远程     | 2 m 21 s    | 6 m 16 s  | 24 m 07 s    | 62 m 56 s  |
| MySQL      | 远程     | 3 m 03 s    | 17 m 48 s | 32 m 38 s    | 175 m 35 s |
| Sql Server | 远程     | 9 m 04 s    | 25 m 36 s | 87 m 32 s    | 391 m 50 s |



### 根据上述测试结果得出 CDC 方式相比 原生性能损耗

计算方式： $$(\frac {CDC耗时-原生耗时} {原生耗时} )\%= 性能损耗$$

|            | CSV Path | Export sf10 | Load sf10 | Export sf100 | Load sf100 |
| ---------- | -------- | ----------- | --------- | ------------ | ---------- |
| PostgreSQL | 远程     | 0.7%        | -2.5%     | -0.4%        | -0.8%      |
| MySQL      | 远程     | -6.6%       | -14.49%   | 2.9%         | -14.6%     |
| Sql Server | 远程     | 5.2%        | -6.4%     | 2.5%         | 6.6%       |

