-- create table 

use test_db;

Create Table If Not Exists 'colume_type'(
            col_int int,
            col_tinyint tinyint,
            col_smallint smallint,
            col_float float,
            col_double double,
            col_decimal decimal ,
            col_char char(256),
            col_varchar varchar(256),
            col_text text,
            col_mediumtext mediumtext,
            col_date date,
            col_time time,
            col_datetime datetime,
            col_linestring linestring
)Engine InnoDB;




-- select some information from metadata


select TABLE_catalo,
TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,ORDINAL_POSITION,IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE,CHARACTER_SET_NAME, COLLATION_NAME 
from information_schema.COLUMNS 
-- where TABLE_SCHEMA not in ('INFORMATION_SCHEMA','SYS','PERFORMANCE_SCHEMA','MYSQL')
where TABLE_NAME in ('colume_type')
ORDER BY table_name,ordinal_position


-- 查看是否是大小写

show Variables like '%table_names'