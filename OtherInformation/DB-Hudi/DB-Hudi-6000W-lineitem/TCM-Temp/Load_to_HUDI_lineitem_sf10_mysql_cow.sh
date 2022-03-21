hdfs dfs -mkdir -p hdfs:///HudiTest/
hdfs dfs -rm -r /HudiTest/lineitem_sf10_mysql_cow
time hdfs dfs -put -f ./TCM-Temp/Export_from_MYSQL_lineitem_sf10_mysql.csv hdfs:///HudiTest/
head -5 ./TCM-Temp/Export_from_MYSQL_lineitem_sf10_mysql.csv > ./TCM-Temp/Export_from_MYSQL_lineitem_sf10_mysql.csv_first_5_lines
rm -f ./TCM-Temp/Export_from_MYSQL_lineitem_sf10_mysql.csv

time spark-shell --jars /usr/local/spark/spark-3.1.2-bin-hadoop3.2/jars/hudi-spark3-bundle_2.12-0.9.0.jar  --packages org.apache.spark:spark-avro_2.12:3.0.1 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --master spark://192.168.30.221:7078 --conf spark.dymaicAllcation.enabled=true --conf spark.default.parallelism=100 -i ./TCM-Temp/Load_CSV_to_hudi_COPY_ON_WRITE.scala