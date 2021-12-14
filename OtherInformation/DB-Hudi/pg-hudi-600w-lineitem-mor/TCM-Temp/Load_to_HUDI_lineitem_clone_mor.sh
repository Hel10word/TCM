hdfs dfs -mkdir -p hdfs:///HudiTest/
hdfs dfs -rm -r /HudiTest/lineitem_clone_mor
time hdfs dfs -put -f ./TCM-Temp/Export_from_POSTGRES_lineitem_clone.csv hdfs:///HudiTest/

time spark-shell \
--jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar \
--master local[2] \
--driver-class-path $HADOOP_CONF_DIR \
--deploy-mode client \
--driver-memory 2G \
--executor-memory 2G \
--num-executors 2 \
--packages org.apache.spark:spark-avro_2.11:2.4.4 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
-i ./TCM-Temp/Load_CSV_to_hudi_MERGE_ON_READ.scala