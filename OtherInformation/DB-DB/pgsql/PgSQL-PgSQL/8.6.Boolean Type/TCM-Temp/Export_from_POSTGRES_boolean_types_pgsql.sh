psql postgres://postgres:@192.168.30.155/test_db -c  "\copy (select pgboolean from boolean_types_pgsql) to './TCM-Temp/POSTGRES_to_POSTGRES_boolean_types_pgsql_p2p.csv' with DELIMITER ',';" 2>&1