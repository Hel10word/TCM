mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "insert into numeric_types_mysql_1006902_temp select myinteger,mysmallint,mydecimal,mynumeric,myfloat,myreal,mydouble_percision,myint,mydec,myfixed,mydouble,mybit,mytinyint,mymediumint,mybigint,mybool,myboolean from numeric_types_mysql;" 2>&1
mysqlsh -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "util.exportTable('numeric_types_mysql_1006902_temp','./TCM-Temp/MYSQL_to_MYSQL_numeric_types_m2m_clone.csv',{linesTerminatedBy:'\n',fieldsTerminatedBy:','})" 2>&1
mysql -h 192.168.30.148 -P 3306 -uroot -proot --database test_db -e "drop table numeric_types_mysql_1006902_temp" 2>&1