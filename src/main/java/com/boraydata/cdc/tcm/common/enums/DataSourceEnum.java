package com.boraydata.cdc.tcm.common.enums;

import com.boraydata.cdc.tcm.exception.TCMException;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Define some SQL query to select table info.
 * add new objects need to follow the naming convention DatasourceConnectionFactory
 * because the name is used to get the content protocol in {@link com.boraydata.cdc.tcm.common.DatasourceConnectionFactory} #dscPropers
 * e.g.{ORACLE DB2 MYSQL POSTGRESQL VOLTDB GREENPLUM MEMSQL RDP HIVE}
 * @author bufan
 * @date 2021/8/24
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
            MySQLContent.CHARACTER_MAXIMUM_LENGTH,
            MySQLContent.NUMERIC_PRECISION,
            MySQLContent.NUMERIC_SCALE,
            MySQLContent.DATETIME_PRECISION,
            MySQLContent.SQL_TABLE_INFO_BY_TABLE_NAME,
            MySQLContent.SQL_TABLE_INFO_BY_CATALOG,
            MySQLContent.SQL_ALL_TABLE_INFO,
            MySQLContent.PRIMARY_KEY_NAME,
            MySQLContent.SQL_GET_PRIMARY_KEYS
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
            PostgreSQLContent.CHARACTER_MAXIMUM_LENGTH,
            PostgreSQLContent.NUMERIC_PRECISION,
            PostgreSQLContent.NUMERIC_SCALE,
            PostgreSQLContent.DATETIME_PRECISION,
            PostgreSQLContent.SQL_TABLE_INFO_BY_TABLE_NAME,
            PostgreSQLContent.SQL_TABLE_INFO_BY_CATALOG,
            PostgreSQLContent.SQL_ALL_TABLE_INFO,
            PostgreSQLContent.PRIMARY_KEY_NAME,
            PostgreSQLContent.SQL_GET_PRIMARY_KEYS
            ),
        SQLSERVER(
            SQLServerContent.TABLE_CATALOG,
            SQLServerContent.TABLE_SCHEMA,
            SQLServerContent.TABLE_NAME,
            SQLServerContent.COLUMN_NAME,
            SQLServerContent.DATA_TYPE,
            SQLServerContent.UDT_TYPE,
            SQLServerContent.ORDINAL_POSITION,
            SQLServerContent.IS_NULLABLE,
            SQLServerContent.CHARACTER_MAXIMUM_LENGTH,
            SQLServerContent.NUMERIC_PRECISION,
            SQLServerContent.NUMERIC_SCALE,
            SQLServerContent.DATETIME_PRECISION,
            SQLServerContent.SQL_TABLE_INFO_BY_TABLE_NAME,
            SQLServerContent.SQL_TABLE_INFO_BY_CATALOG,
            SQLServerContent.SQL_ALL_TABLE_INFO,
            SQLServerContent.PRIMARY_KEY_NAME,
            SQLServerContent.SQL_GET_PRIMARY_KEYS
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
            null,
            null,
            null
            ),
        KAFKA(
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
            null,
            null,
            null
        ),
        HADOOP(
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
        if(value.equals(DataSourceEnum.MYSQL.toString()) || value.equalsIgnoreCase("mysql") || value.equalsIgnoreCase("rpdsql"))
            return DataSourceEnum.MYSQL;
        else if(value.equals(DataSourceEnum.POSTGRESQL.toString()) || value.equalsIgnoreCase("postgresql"))
            return DataSourceEnum.POSTGRESQL;
        else if(value.equals(DataSourceEnum.POSTGRESQL.toString()) || value.equalsIgnoreCase("sqlserver"))
            return DataSourceEnum.SQLSERVER;
        else if(value.equals(DataSourceEnum.HUDI.toString()) || value.equalsIgnoreCase("hudi") || value.equalsIgnoreCase("hive"))
            return DataSourceEnum.HUDI;
        else if(value.equals(DataSourceEnum.KAFKA.toString()) || value.equalsIgnoreCase("kafka"))
            return DataSourceEnum.KAFKA;
        else if(value.equals(DataSourceEnum.HADOOP.toString()) || value.equalsIgnoreCase("hadoop"))
            return DataSourceEnum.HADOOP;
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
    public final String CharacterMaximumLength;
    public final String NumericPrecision;
    public final String NumericScale;
    public final String DatetimePrecision;
//    public final String ColumnComment;
    public final String SQL_TableInfoByTableName;
    public final String SQL_TableInfoByCatalog;
    public final String SQL_AllTableInfo;
    public final String PrimaryKeyName;
    public final String SQL_GetPrimaryKeys;


    DataSourceEnum(String TableCatalog,
                   String TableSchema,
                   String TableName,
                   String ColumnName,
                   String DataType,
                   String UdtType,
                   String OrdinalPosition,
                   String IsNullAble,
                   String CharacterMaximumLength,
                   String NumericPrecision,
                   String NumericScale,
                   String DatetimePrecision,
//                   String ColumnComment,
                   String SQL_TableInfoByTableName,
                   String SQL_TableInfoByCatalog,
                   String SQL_AllTableInfo,
                   //PRIMARY_KEY_NAME
                   String PrimaryKeyName,
                   String SQL_GetPrimaryKeys){
        this.TableCatalog = TableCatalog;
        this.TableSchema = TableSchema;
        this.TableName = TableName;
        this.ColumnName = ColumnName;
        this.DataType = DataType;
        this.UdtType = UdtType;
        this.OrdinalPosition = OrdinalPosition;
        this.IsNullAble = IsNullAble;
        this.CharacterMaximumLength = CharacterMaximumLength;
        this.NumericPrecision = NumericPrecision;
        this.NumericScale = NumericScale;
        this.DatetimePrecision = DatetimePrecision;
//        this.ColumnComment = ColumnComment;
        this.SQL_TableInfoByTableName = SQL_TableInfoByTableName;
        this.SQL_TableInfoByCatalog = SQL_TableInfoByCatalog;
        this.SQL_AllTableInfo = SQL_AllTableInfo;
        this.PrimaryKeyName = PrimaryKeyName;
        this.SQL_GetPrimaryKeys = SQL_GetPrimaryKeys;
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
        private static final String CHARACTER_MAXIMUM_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
        private static final String NUMERIC_PRECISION = "NUMERIC_PRECISION";
        private static final String NUMERIC_SCALE = "NUMERIC_SCALE";
        private static final String DATETIME_PRECISION = "DATETIME_PRECISION";
        private static final String COLUMN_COMMENT = "COLUMN_COMMENT";
        private static final String SELECT_TABLE_COLUMN =
                "select "+TABLE_CATALOG+","+TABLE_SCHEMA+","+TABLE_NAME+","+COLUMN_NAME+","+DATA_TYPE+","+UDT_TYPE+","+ORDINAL_POSITION+","+IS_NULLABLE+","+ CHARACTER_MAXIMUM_LENGTH +","+ NUMERIC_PRECISION +","+ NUMERIC_SCALE +","+DATETIME_PRECISION;
//                " select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,ORDINAL_POSITION,IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE,CHARACTER_SET_NAME, COLLATION_NAME ";
        private static final String SELECT_TABLE_FROM = " from information_schema.COLUMNS ";
        private static final String WHERE = " where ";
        private static final String AND = " and ";
        // because mysql only have doubly structures,all catalog is 'def'   e.g: def.testDB.testTable
        private static final String WHERE_TABLE_CATALOG = " "+TABLE_SCHEMA+" in (?) ";
        private static final String WHERE_TABLE_NAME = " "+TABLE_NAME+" in (?) ";
        private static final String WHERE_ALL_TABLE =
                " "+TABLE_SCHEMA+" not in ('INFORMATION_SCHEMA','SYS','PERFORMANCE_SCHEMA','MYSQL') ";
        private static final String ORDER_BY = " ORDER BY "+TABLE_NAME+","+ORDINAL_POSITION+" ";

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

        private static final String PRIMARY_KEY_NAME = "CONSTRAINT_NAME";
        private static final String SQL_GET_PRIMARY_KEYS =
             "SELECT k.COLUMN_NAME as "+COLUMN_NAME+" ,t.CONSTRAINT_NAME as "+PRIMARY_KEY_NAME+",k.ORDINAL_POSITION\n" +
                     "FROM information_schema.table_constraints t\n" +
                     "JOIN information_schema.key_column_usage k\n" +
                     "USING(constraint_name,table_schema,table_name)\n" +
                     "WHERE t.constraint_type='PRIMARY KEY'\n" +
                     "  AND t.CONSTRAINT_CATALOG = ?\n" +
                     "  AND t.table_schema = ?\n" +
                     "  AND t.table_name = ?\n" +
                     " order by k.ORDINAL_POSITION ;";
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
        private static final String CHARACTER_MAXIMUM_LENGTH = "character_maximum_length";
        private static final String NUMERIC_PRECISION = "numeric_precision";
        private static final String NUMERIC_SCALE = "numeric_scale";
        private static final String DATETIME_PRECISION = "datetime_precision";
//        private static final String COLUMN_COMMENT = "COLUMN_COMMENT";
        private static final String SELECT_TABLE_COLUMN =
                "select "+TABLE_CATALOG+","+TABLE_SCHEMA+","+TABLE_NAME+","+COLUMN_NAME+","+DATA_TYPE+","+UDT_TYPE+","+ORDINAL_POSITION+","+IS_NULLABLE+","+ CHARACTER_MAXIMUM_LENGTH +","+ NUMERIC_PRECISION +","+ NUMERIC_SCALE +","+DATETIME_PRECISION;
//                " select table_catalog,table_schema,table_name,column_name,data_type,ordinal_position,is_nullable,numeric_precision,numeric_precision_radix,numeric_scale,character_set_name,collation_name ";
        private static final String SELECT_TABLE_FROM = " from information_schema.columns ";
        private static final String WHERE = " where ";
        private static final String AND = " and ";
        private static final String WHERE_TABLE_CATALOG = " "+TABLE_CATALOG+" in  (?) ";
        private static final String WHERE_TABLE_NAME = " "+TABLE_NAME+" in (?) ";
        private static final String WHERE_ALL_TABLE =
                " "+TABLE_CATALOG+" not in  ('information_schema','pg_catalog','pg_toast_temp_1','pg_temp_1','pg_toast') ";
        private static final String ORDER_BY = " ORDER BY "+TABLE_NAME+","+ORDINAL_POSITION+" ";

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

        private static final String PRIMARY_KEY_NAME = "conname";
        private static final String SQL_GET_PRIMARY_KEYS =
                "with pg_pk as (\n" +
                "    select conrelid,conname,conkey,pg_get_constraintdef(oid) as constraintdef,unnest(conkey) as pk_order\n" +
                "        FROM pg_constraint\n" +
                "    where\n" +
                "        conrelid = ?::regclass \n" +
                ")\n" +
                "select pg_pk.conrelid,pg_pk.conname as "+PRIMARY_KEY_NAME+",constraintdef,a.attname as "+COLUMN_NAME+",a.attnum \n" +
                "    from pg_pk \n" +
                "    left join pg_attribute a on a.attrelid = pg_pk.conrelid and a.attnum = pg_pk.pk_order";
    }

    /**
     * @author: bufan
     * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-linux-ver15"></a>
     */
    private static class SQLServerContent {
        private static final String TABLE_CATALOG = "TABLE_CATALOG";
        private static final String TABLE_SCHEMA = "TABLE_SCHEMA";
        private static final String TABLE_NAME = "TABLE_NAME";
        private static final String COLUMN_NAME = "COLUMN_NAME";
        private static final String DATA_TYPE = "DATA_TYPE";
        private static final String UDT_TYPE = "DATA_TYPE";
        private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
        private static final String IS_NULLABLE = "IS_NULLABLE";
        private static final String CHARACTER_MAXIMUM_LENGTH = "CHARACTER_MAXIMUM_LENGTH";
        private static final String NUMERIC_PRECISION = "NUMERIC_PRECISION";
        private static final String NUMERIC_SCALE = "NUMERIC_SCALE";
        private static final String DATETIME_PRECISION = "DATETIME_PRECISION";
        //        private static final String COLUMN_COMMENT = "COLUMN_COMMENT";
        private static final String SELECT_TABLE_COLUMN =
                "select "+TABLE_CATALOG+","+TABLE_SCHEMA+","+TABLE_NAME+","+COLUMN_NAME+","+DATA_TYPE+","+UDT_TYPE+","+ORDINAL_POSITION+","+IS_NULLABLE+","+CHARACTER_MAXIMUM_LENGTH+","+NUMERIC_PRECISION+","+NUMERIC_SCALE+","+DATETIME_PRECISION;
        //                " select table_catalog,table_schema,table_name,column_name,data_type,ordinal_position,is_nullable,numeric_precision,numeric_precision_radix,numeric_scale,character_set_name,collation_name ";
        private static final String SELECT_TABLE_FROM = " from INFORMATION_SCHEMA.COLUMNS ";
        private static final String WHERE = " where ";
        private static final String AND = " and ";
        private static final String WHERE_TABLE_CATALOG = " "+TABLE_CATALOG+" in  (?) ";
        private static final String WHERE_TABLE_NAME = " "+TABLE_NAME+" in (?) ";
        private static final String WHERE_ALL_TABLE =
                " "+TABLE_CATALOG+" not in  ('INFORMATION_SCHEMA','db_owner','db_accessadmin','db_securityadmin','db_ddladmin','db_backupoperator','db_datareader','db_datawriter','db_denydatareader','db_denydatawriter') ";
        private static final String ORDER_BY = " ORDER BY "+TABLE_NAME+","+ORDINAL_POSITION+" ";

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

        private static final String PRIMARY_KEY_NAME = "CONSTRAINT_NAME";
        private static final String SQL_GET_PRIMARY_KEYS =
                "SELECT column_name as "+COLUMN_NAME+",TC.CONSTRAINT_NAME as "+PRIMARY_KEY_NAME+",KU.ORDINAL_POSITION\n" +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC\n" +
                "INNER JOIN\n" +
                "    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU\n" +
                "          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND\n" +
                "             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND \n" +
                "             KU.CONSTRAINT_CATALOG = ? AND \n" +
                "             KU.CONSTRAINT_SCHEMA  = ? AND \n" +
                "             KU.table_name = ?\n" +
                "ORDER BY KU.ORDINAL_POSITION,KU.TABLE_NAME;";
    }

}
