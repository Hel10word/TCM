psql postgres://postgres:@192.168.30.155/test_db -c  "\copy numeric_types_pgsql_p2p from './TCM-Temp/POSTGRES_to_POSTGRES_numeric_types_pgsql_p2p.csv' with DELIMITER ',';" 2>&1