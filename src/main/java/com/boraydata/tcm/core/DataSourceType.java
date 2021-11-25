package com.boraydata.tcm.core;


import com.boraydata.tcm.exception.TCMException;

/** Save or define some SQL to Select table info
 * @author bufan
 * @data 2021/8/24
 */
public enum DataSourceType {

/**
 * add new objects need to follow the naming convention com.boraydata.tcm.configuration.DatasourceConnectionFactory
 * because the name is used to get the content protocol in *.utils.DatasourceConnectionFactory.dscPropers
 * e.g.{ORACLE DB2 MYSQL POSTGRES VOLTDB GREENPLUM MEMSQL RDP SPARK}
 */

// should to specify define the field name to be queried, because the field names may not be uniform for different databases
    MYSQL(
        MySQLContent.TABLE_CATALOG,
        MySQLContent.TABLE_SCHEMA,
        MySQLContent.TABLE_NAME,
        MySQLContent.COLUMN_NAME,
        MySQLContent.DATA_TYPE,
        MySQLContent.UDT_TYPE,
        MySQLContent.ORDINAL_POSITION,
        MySQLContent.IS_NULLABLE,
        MySQLContent.CHAR_MAX_LENGTH,
        MySQLContent.NUMERIC_PRECISION_M,
        MySQLContent.NUMERIC_PRECISION_D,
        MySQLContent.DATETIME_PRECISION,
        MySQLContent.SQL_TABLE_INFO_BY_TABLE_NAME,
        MySQLContent.SQL_TABLE_INFO_BY_CATALOG,
        MySQLContent.SQL_ALL_TABLE_INFO
            ),
    POSTGRES(
        PGSQLContent.TABLE_CATALOG,
        PGSQLContent.TABLE_SCHEMA,
        PGSQLContent.TABLE_NAME,
        PGSQLContent.COLUMN_NAME,
        PGSQLContent.DATA_TYPE,
        PGSQLContent.UDT_TYPE,
        PGSQLContent.ORDINAL_POSITION,
        PGSQLContent.IS_NULLABLE,
        PGSQLContent.CHAR_MAX_LENGTH,
        PGSQLContent.NUMERIC_PRECISION_M,
        PGSQLContent.NUMERIC_PRECISION_D,
        PGSQLContent.DATETIME_PRECISION,
        PGSQLContent.SQL_TABLE_INFO_BY_TABLE_NAME,
        PGSQLContent.SQL_TABLE_INFO_BY_CATALOG,
        PGSQLContent.SQL_ALL_TABLE_INFO
            ),
    HUDI(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    public static DataSourceType getTypeByStr(String str){
        if(str.equals(DataSourceType.MYSQL.toString()))
            return DataSourceType.MYSQL;
        else if(str.equals(DataSourceType.POSTGRES.toString()))
            return DataSourceType.POSTGRES;
        else if(str.equals(DataSourceType.HUDI.toString()))
            return DataSourceType.HUDI;
        throw new TCMException("Failed to get DataSourceType, you must fill in the correct , are not '"+str+'\'');
    }

//     Query table info by table name and table schema in different DB

    public final String TableCatalog;
    public final String TableSchema;
    public final String TableName;
    public final String ColumnName;
    public final String DataType;
    public final String UdtType;
    public final String OrdinalPosition;
    public final String IsNullAble;
    public final String CharMaxLength;
    public final String NumericPrecisionM;
    public final String NumericPrecisionD;
    public final String DatetimePrecision;
    public final String SQL_TableInfoByTableName;
    public final String SQL_TableInfoByCatalog;
    public final String SQL_AllTableInfo;

    DataSourceType(String TableCatalog,
                   String TableSchema,
                   String TableName,
                   String ColumnName,
                   String DataType,
                   String UdtType,
                   String OrdinalPosition,
                   String IsNullAble,
                   String CharMaxLength,
                   String NumericPrecisionM,
                   String NumericPrecisionD,
                   String DatetimePrecision,
                   String SQL_TableInfoByTableName,
                   String SQL_TableInfoByCatalog,
                   String SQL_AllTableInfo){
        this.TableCatalog = TableCatalog;
        this.TableSchema = TableSchema;
        this.TableName = TableName;
        this.ColumnName = ColumnName;
        this.DataType = DataType;
        this.UdtType = UdtType;
        this.OrdinalPosition = OrdinalPosition;
        this.IsNullAble = IsNullAble;
        this.CharMaxLength = CharMaxLength;
        this.NumericPrecisionM = NumericPrecisionM;
        this.NumericPrecisionD = NumericPrecisionD;
        this.DatetimePrecision = DatetimePrecision;
        this.SQL_TableInfoByTableName = SQL_TableInfoByTableName;
        this.SQL_TableInfoByCatalog = SQL_TableInfoByCatalog;
        this.SQL_AllTableInfo = SQL_AllTableInfo;
    }



 /**
  * @描述: ========================================  SQL Query  ======================================================================
  * @author: bufan
  * @date: 2021/8/31 9:41
  */
//    https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/information-schema-columns-table.html
    private static class MySQLContent  {
        private static final String TABLE_CATALOG = "TABLE_CATALOG";
        private static final String TABLE_SCHEMA = "TABLE_SCHEMA";
        private static final String TABLE_NAME = "TABLE_NAME";
        private static final String COLUMN_NAME = "COLUMN_NAME";
        private static final String DATA_TYPE = "COLUMN_TYPE";
        private static final String UDT_TYPE = "DATA_TYPE";
        private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
        private static final String IS_NULLABLE = "IS_NULLABLE";
        private static final String CHAR_MAX_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
        private static final String NUMERIC_PRECISION_M = "NUMERIC_PRECISION";
        private static final String NUMERIC_PRECISION_D = "NUMERIC_SCALE";
        private static final String DATETIME_PRECISION = "DATETIME_PRECISION";
        private static final String SELECT_TABLE_COLUMN =
                "select "+TABLE_CATALOG+","+TABLE_SCHEMA+","+TABLE_NAME+","+COLUMN_NAME+","+DATA_TYPE+","+UDT_TYPE+","+ORDINAL_POSITION+","+IS_NULLABLE+","+CHAR_MAX_LENGTH+","+NUMERIC_PRECISION_M+","+NUMERIC_PRECISION_D+","+DATETIME_PRECISION;
//                " select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,ORDINAL_POSITION,IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE,CHARACTER_SET_NAME, COLLATION_NAME ";
        private static final String SELECT_TABLE_FROM = " from information_schema.COLUMNS ";
        private static final String WHERE = " where ";
        private static final String AND = " and ";
        // because mysql only have doubly structures,all catalog is 'def'   e.g: def.testDB.testTable
        private static final String WHERE_TABLE_CATALOG = " TABLE_SCHEMA in (?) ";
        private static final String WHERE_TABLE_NAME = " TABLE_NAME in (?) ";
        private static final String WHERE_ALL_TABLE =
                " TABLE_SCHEMA not in ('INFORMATION_SCHEMA','SYS','PERFORMANCE_SCHEMA','MYSQL') ";
        private static final String ORDER_BY = " ORDER BY TABLE_NAME,ORDINAL_POSITION ";

     private static final String SQL_TABLE_INFO_BY_TABLE_NAME =
             SELECT_TABLE_COLUMN+
                     SELECT_TABLE_FROM+
                     WHERE+WHERE_TABLE_NAME+
                     ORDER_BY+";";
     private static final String SQL_TABLE_INFO_BY_CATALOG =
             SELECT_TABLE_COLUMN+
                     SELECT_TABLE_FROM+
                     WHERE+WHERE_TABLE_CATALOG+
                     ORDER_BY+";";
     private static final String SQL_ALL_TABLE_INFO =
             SELECT_TABLE_COLUMN+
                     SELECT_TABLE_FROM+
                     WHERE+WHERE_ALL_TABLE+
                     ORDER_BY+";";
    }

    //        http://www.postgres.cn/docs/12/infoschema-columns.html
    private static class PGSQLContent  {
        private static final String TABLE_CATALOG = "table_catalog";
        private static final String TABLE_SCHEMA = "table_schema";
        private static final String TABLE_NAME = "table_name";
        private static final String COLUMN_NAME = "column_name";
        private static final String DATA_TYPE = "data_type";
        private static final String UDT_TYPE = "udt_name";
        private static final String ORDINAL_POSITION = "ordinal_position";
        private static final String IS_NULLABLE = "is_nullable";
        private static final String CHAR_MAX_LENGTH = "character_maximum_length";
        private static final String NUMERIC_PRECISION_M = "numeric_precision";
        private static final String NUMERIC_PRECISION_D = "numeric_scale";
        private static final String DATETIME_PRECISION = "datetime_precision";
        private static final String SELECT_TABLE_COLUMN =
                "select "+TABLE_CATALOG+","+TABLE_SCHEMA+","+TABLE_NAME+","+COLUMN_NAME+","+DATA_TYPE+","+UDT_TYPE+","+ORDINAL_POSITION+","+IS_NULLABLE+","+CHAR_MAX_LENGTH+","+NUMERIC_PRECISION_M+","+NUMERIC_PRECISION_D+","+DATETIME_PRECISION;
//                " select table_catalog,table_schema,table_name,column_name,data_type,ordinal_position,is_nullable,numeric_precision,numeric_precision_radix,numeric_scale,character_set_name,collation_name ";
        private static final String SELECT_TABLE_FROM = " from information_schema.columns ";
        private static final String WHERE = " where ";
        private static final String AND = " and ";
        private static final String WHERE_TABLE_CATALOG = " table_catalog in  (?) ";
        private static final String WHERE_TABLE_NAME = " table_name in (?) ";
        private static final String WHERE_ALL_TABLE =
                " table_catalog not in  ('information_schema','pg_catalog','pg_toast_temp_1','pg_temp_1','pg_toast') ";
        private static final String ORDER_BY = " ORDER BY table_name,ordinal_position ";

        private static final String SQL_TABLE_INFO_BY_TABLE_NAME =
                SELECT_TABLE_COLUMN+
                        SELECT_TABLE_FROM+
                        WHERE+WHERE_TABLE_NAME+
                        ORDER_BY+";";
        private static final String SQL_TABLE_INFO_BY_CATALOG =
                SELECT_TABLE_COLUMN+
                        SELECT_TABLE_FROM+
                        WHERE+WHERE_TABLE_CATALOG+
                        ORDER_BY+";";
        private static final String SQL_ALL_TABLE_INFO =
                SELECT_TABLE_COLUMN+
                        SELECT_TABLE_FROM+
                        WHERE+WHERE_ALL_TABLE+
                        ORDER_BY+";";
    }

}
