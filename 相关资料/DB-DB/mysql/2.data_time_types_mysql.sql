
-- https://dev.mysql.com/doc/refman/5.7/en/date-and-time-type-syntax.html

CREATE TABLE  data_time_types_mysql(
    mydate DATE,
    mytime TIME,
    mytime6 TIME(6),
    mydatetime DATETIME,
    mydatetime6 DATETIME(6),
    mytimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    mytimestamp6 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    myyear YEAR
);

INSERT INTO `data_time_types_mysql` (`mydate`, `mytime`, `mytime6`, `mydatetime`, `mydatetime6`, `mytimestamp`, `mytimestamp6`, `myyear`) VALUES ('1000-01-01', '-838:59:59', '-838:59:59.000000', '1000-01-01 00:00:00', '1000-01-01 00:00:00.000000', '2021-10-25 11:13:45', '2021-10-25 11:14:14.000000', 1901);
INSERT INTO `data_time_types_mysql` (`mydate`, `mytime`, `mytime6`, `mydatetime`, `mydatetime6`, `mytimestamp`, `mytimestamp6`, `myyear`) VALUES ('9999-12-31', '838:59:59', '838:59:59.000000', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999', '2021-10-25 11:18:42', '2021-10-25 11:18:42.002447', 2155);