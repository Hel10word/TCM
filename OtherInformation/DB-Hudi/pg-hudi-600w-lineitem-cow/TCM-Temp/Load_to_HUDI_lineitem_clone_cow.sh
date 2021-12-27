hdfs dfs -mkdir -p hdfs:///HudiTest/
hdfs dfs -rm -r /HudiTest/lineitem_clone_cow
time hdfs dfs -put -f ./TCM-Temp/Export_from_POSTGRES_lineitem_clone.csv hdfs:///HudiTest/

time spark-shell --jars /opt/CDC/spark/jars/hudi-spark-bundle_2.11-0.8.0.jar --driver-class-path $HADOOP_CONF_DIR --packages org.apache.spark:spark-avro_2.11:2.4.4 --master yarn --deploy-mode client --driver-memory 4g --driver-cores 1 --num-executors 10 --executor-memory 4g --executor-cores 2 --conf spark.default.parallelism=40  -i ./TCM-Temp/Load_CSV_to_hudi_COPY_ON_WRITE.scala