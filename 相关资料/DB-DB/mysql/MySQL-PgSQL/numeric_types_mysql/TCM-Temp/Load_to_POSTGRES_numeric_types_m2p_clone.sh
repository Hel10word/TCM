psql postgres://postgres:@192.168.30.155/test_db -c  "\copy numeric_types_m2p_clone from './TCM-Temp/MYSQL_to_POSTGRES_numeric_types_m2p_clone.csv' with DELIMITER ',';" 2>&1