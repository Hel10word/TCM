-- Create a table with as many fields as possible, robin provided
CREATE TABLE robin_types_full(
                          col_bigint bigint,
                          col_bigserial bigserial,
                          col_bit bit,
                          col_bitvarying bit varying,
                          col_boolean boolean,
                          col_box box,
                          col_bytea bytea,
                          col_character character(10),
                          col_charactervarying character varying(100),
                          col_cidr cidr,
                          col_circle circle,
                          col_date date,
                          col_doubleprecision double precision,
                          col_inet inet,
                          col_integer integer,
                          col_interval interval,
                          col_json json,
                          col_jsonb jsonb,
                          col_line line,
                          col_lseg lseg,
                          col_macaddr macaddr,
                          col_macaddr8 macaddr8,
                          col_money money,
                          col_numeric numeric,
                          col_path path,
                          col_pg_lsn pg_lsn,
                          col_point point,
                          col_polygon polygon,
                          col_real real,
                          col_smallint smallint,
                          col_smallserial smallserial,
                          col_serial serial,
                          col_text text,
                          col_time time,
                          col_timez time with time zone,
                          col_timestamp timestamp,
                          col_timestampz timestamp with time zone,
                          col_tsquery tsquery,
                          col_tsvector tsvector,
                          col_txid_snapshot txid_snapshot,
                          col_uuid uuid,
                          col_xml xml
);

-- insert a piece of data
INSERT INTO robin_types_full (col_bigint,col_bigserial,col_bit,col_bitvarying,col_boolean,col_box,col_bytea,col_character,col_charactervarying,col_cidr,
                              col_circle,col_date,col_doubleprecision,col_inet,col_integer,col_interval,col_json,col_jsonb,col_line,col_lseg,
                              col_macaddr,col_macaddr8,col_money,col_numeric,col_path,col_pg_lsn,col_point,col_polygon,col_real,col_smallint,
                              col_smallserial,col_serial,col_text,col_time,col_timez,col_timestamp,col_timestampz,col_tsquery,col_tsvector,col_txid_snapshot,
                              col_uuid,col_xml)
VALUES (
9223372036854775807,9223372036854775807,B'1',B'101',TRUE,'((0,0),(1,1))','abc \153\154\155 \052\251\124','aaaaaaaaaa','aaaaaaaaaa','192.168.100.128/25',
'<(1,1),10>','2020-12-31',99.99999999999999,'127.0.0.1',2147483647,'1 year 2 months 3 days 4 hours 5 minutes 6 seconds','{"tags":[{"term":"paris"},{"term":"food"}]}','[1, 2, 3]','{1,2,3}','((1,1),(2,2))','08:00:2b:01:02:03','08:00:2b:01:02:03:04:05',
92233720368547758.07,9999999.99999999999999999999999,'((1,1),(2,2),(3,3))', '0/1656FE0' ,'(1,1)','((1,1),(2,2),(3,3))',99.99999,32767,32767,2147483647,
'aaaaaaaaaaaaaaaaaaaaaaaa','11:12:59','11:12:59 -8:00','2020-12-31 11:12:59','2020-12-31 11:12:59 -8:00', 'fat:AB & cat',
default,default,'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',xml '<foo>bar</foo>'
);


-- insert a piece of data

INSERT INTO robin_types_full (col_bigint,col_bigserial,col_bit,col_bitvarying,col_boolean,col_box,col_bytea,col_character,col_charactervarying,col_cidr,
                                                            col_circle,col_date,col_doubleprecision,col_inet,col_integer,col_interval,col_json,col_jsonb,col_line,col_lseg,
                                                            col_macaddr,col_macaddr8,col_money,col_numeric,col_path,col_pg_lsn,col_point,col_polygon,col_real,col_smallint,
                                                            col_smallserial,col_serial,col_text,col_time,col_timez,col_timestamp,col_timestampz,col_tsquery,col_tsvector,col_txid_snapshot,
                                                            col_uuid,col_xml)
VALUES (
-9223372036854775808,1,B'1',B'101',FALSE,'((0,0),(1,1))','abc \153\154\155 \052\251\124','z','z','2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128',
'<(1,1),1>','2020-12-31',0.00000000000001,'2001:4f8:3:ba:2e0:81ff:fe22:d1f1',-2147483648,'0 year 0 months 0 days 0 hours 0 minutes 1 seconds',
'{}','{"foo": {"bar": "baz"}}','{1,1,1}','((0,1),(1,0))','08:00:2b:01:02:03','08:00:2b:01:02:03:04:05',
-92233720368547758.08,0.00000000000000000000001,'((1,1),(2,2),(3,3))', '0/1656FE0' ,'(0,0)','((1,1),(2,2),(3,3))',0.00001,-32768,1,1,
'zzzzzzzzzzzzzzzzzz','11:12:59','11:12:59 -8:00','2020-01-01 01:01:00','2020-01-01 01:01:00 -8:00','fat:AB & cat',
' a fat cat sat on a mat and ate a fat rat',default,'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',xml '<foo>bar</foo>');


-- select some information from metadata

select TABLE_catalog,table_schema,table_name,column_name,data_type,ordinal_position,is_nullable,numeric_precision,numeric_precision_radix,numeric_scale,character_set_name,collation_name 
from information_schema.columns 
-- where table_catalog not in  ('information_schema','pg_catalog','pg_toast_temp_1','pg_temp_1','pg_toast')
where TABLE_NAME in ('pg_colume_type')
ORDER BY table_name,ordinal_position
