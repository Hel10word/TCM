mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "load data local infile './TCM-Temp/POSTGRES_to_MYSQL_datatime_types_pgsql_p2m.csv' into table datatime_types_pgsql_p2m fields terminated by ',';" 2>&1