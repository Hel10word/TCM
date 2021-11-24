-- https://dev.mysql.com/doc/refman/5.7/en/mysql-command-options.html#option_mysql_binary-as-hex

CREATE TABLE  byte_types_mysql(
    mybit1 BIT(1),
    mybit5 BIT(5),
    mybinary BINARY(255),
    myvarbinary VARBINARY(8126),
    myblob BLOB,
    mymediumblob MEDIUMBLOB,
    mylongblob LONGBLOB
);

insert into byte_types_mysql values(1,1,'a','abcde','abcde','abcde','abcde');
insert into byte_types_mysql values(0,010,1,'abcde','abcde','abcde','abcde');
insert into byte_types_mysql values(0,000,1,11111,11111,11111,11111);
insert into byte_types_mysql values(0,000,1,15,15,15,15);

select mybit1,mybit5,mybinary,myvarbinary,BIN(myblob),BIN(mymediumblob),BIN(mylongblob) from byte_types_mysql;

select mybit1,mybit5,mybinary,HEX(myvarbinary),HEX(myblob),HEX(mymediumblob),HEX(mylongblob) from byte_types_mysql;

select mybit1,mybit5,mybinary,CONCAT("0x",HEX(myvarbinary)) as myvarbinary,HEX(myblob),HEX(mymediumblob),HEX(mylongblob) from byte_types_mysql;