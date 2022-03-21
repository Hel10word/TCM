

(1).DB Export lineitem_sf10 的 CSV 耗时 4min20s 。
(2).将CSV put 到 HDFS 耗时 3min30s。
(3).将 HDFS 上的 CSV Load 到Hudi，使用 MOR 的方式 耗时 8min；使用 COW 的方式耗时 7min40s；

由于，选用的是lineitem_sf10表，MySQL导出时不需要生成临时表，因此导出成CSV的时间与PGSQL相差不大。Hudi这边使用MOR与COW耗时相差也不大。所以DB-Hudi，sf10 的数据总耗时 16分钟。

我尝试了很多手动的Executor参数配置，后来都没有执行成功。于是开启了Executor动态调整，parallelism为100。在3个节点的Spark集群上，总资源为24个CPU、24GB内存，Spark：3.2.12，Hudi：0.9。另外测试结果仅供参考，parallelism 会影响 Spark 的执行时间。下面是运行的命令。

time spark-shell \
--master spark://192.168.30.221:7078 \
--jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar  \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.9.0,org.apache.spark:spark-avro_2.12:3.0.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf spark.dymaicAllcation.enabled=true \
--conf spark.default.parallelism=100 \
-i Load_CSV_to_hudi.scala


！！！！！！！！！！！！！！！！ 如果使用 Executor动态调整 ，在 spark 运行任务的时候，不要运行其他 Spark-Shell ，以免会导致资源不够，影响最终的测试结果。
！！！！！！！！！！！！！！！！ 执行大数据量的数据时，不仅内存需要能够容纳CSV，运行 Spark-Shell 的机器本地硬盘空间也需要容纳。可能是 Spark 会在本地生成相关的缓存。