package com.boraydata.cdc.tcm.common.enums;

import com.boraydata.cdc.tcm.exception.TCMException;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Define some SQL query to select table info.
 * add new objects need to follow the naming convention DatasourceConnectionFactory
 * because the name is used to get the content protocol in {@link com.boraydata.cdc.tcm.common.DatasourceConnectionFactory} #dscPropers
 * e.g.{ORACLE DB2 MYSQL POSTGRESQL VOLTDB GREENPLUM MEMSQL RDP HIVE}
 * @author bufan
 * @data 2021/8/24
 */
public enum DataSourceEnum {

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
        POSTGRESQL(
            PostgreSQLContent.TABLE_CATALOG,
            PostgreSQLContent.TABLE_SCHEMA,
            PostgreSQLContent.TABLE_NAME,
            PostgreSQLContent.COLUMN_NAME,
            PostgreSQLContent.DATA_TYPE,
            PostgreSQLContent.UDT_TYPE,
            PostgreSQLContent.ORDINAL_POSITION,
            PostgreSQLContent.IS_NULLABLE,
            PostgreSQLContent.CHAR_MAX_LENGTH,
            PostgreSQLContent.NUMERIC_PRECISION_M,
            PostgreSQLContent.NUMERIC_PRECISION_D,
            PostgreSQLContent.DATETIME_PRECISION,
            PostgreSQLContent.SQL_TABLE_INFO_BY_TABLE_NAME,
            PostgreSQLContent.SQL_TABLE_INFO_BY_CATALOG,
            PostgreSQLContent.SQL_ALL_TABLE_INFO
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

    @JsonValue
    @Override
    public String toString() {
        return super.toString();
    }

    public static DataSourceEnum valueOfByString(String value){
        if(value.equals(DataSourceEnum.MYSQL.toString()) || value.equalsIgnoreCase("mysql"))
            return DataSourceEnum.MYSQL;
        else if(value.equals(DataSourceEnum.POSTGRESQL.toString()) || value.equalsIgnoreCase("postgresql"))
            return DataSourceEnum.POSTGRESQL;
        else if(value.equals(DataSourceEnum.HUDI.toString()) || value.equalsIgnoreCase("hudi"))
            return DataSourceEnum.HUDI;
        throw new TCMException("Failed to get DataSourceEnum, you must fill in the correct , are not '"+value+'\'');
    }

//

    /**
     * Query table info by table name and table schema in different DB.
     * should to specify define the field name to be queried, because the field names may not be uniform for different databases
     *
     * @author: bufan
     * @date: 2022/5/16
     */
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
//    public final String ColumnComment;
    public final String SQL_TableInfoByTableName;
    public final String SQL_TableInfoByCatalog;
    public final String SQL_AllTableInfo;

    DataSourceEnum(String TableCatalog,
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
//                   String ColumnComment,
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
//        this.ColumnComment = ColumnComment;
        this.SQL_TableInfoByTableName = SQL_TableInfoByTableName;
        this.SQL_TableInfoByCatalog = SQL_TableInfoByCatalog;
        this.SQL_AllTableInfo = SQL_AllTableInfo;
    }



    // ========================================  SQL Query  ======================================================================

    /**
     * @author: bufan
     * @date: 2021/8/31
     * @see <a href="https://dev.mysql.com/doc/mysql-infoschema-excerpt/5.7/en/information-schema-columns-table.html"></a>
     */
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
        private static final String COLUMN_COMMENT = "COLUMN_COMMENT";
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

    /**
     * @author: bufan
     * @date: 2021/8/31
     * @see <a href="http://www.postgres.cn/docs/14/infoschema-columns.html"></a>
     */
    private static class PostgreSQLContent {
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
//        private static final String COLUMN_COMMENT = "COLUMN_COMMENT";
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
