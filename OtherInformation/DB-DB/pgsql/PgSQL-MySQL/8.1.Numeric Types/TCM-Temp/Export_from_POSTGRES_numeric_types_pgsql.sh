psql postgres://postgres:@192.168.30.155/test_db -c  "\copy numeric_types_pgsql to './TCM-Temp/POSTGRES_to_MYSQL_numeric_types_pgsql_p2m.csv' with DELIMITER ',';" 2>&1