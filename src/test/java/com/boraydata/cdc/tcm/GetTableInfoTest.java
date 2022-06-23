package com.boraydata.cdc.tcm;

import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.core.TableCloneManager;
import com.boraydata.cdc.tcm.mapping.MappingTool;
import com.boraydata.cdc.tcm.mapping.MappingToolFactory;
import org.junit.jupiter.api.Test;

import java.sql.*;

/** use JDBC Metadata show table info
 * @author bufan
 * @date 2021/10/20
 */
public class GetTableInfoTest {

    DatabaseConfig mysqlConfig = TestDataProvider.MySQLConfig;

//    DatabaseConfig mysqlConfig = mysql
//            .setDatabasename("test_db")
//            .setDataSourceEnum(DataSourceEnum.MYSQL)
//            .setHost("192.168.30.38")
//            .setPort("3306")
//            .setUsername("root")
//            .setPassword("root")
//            .create();

    DatabaseConfig postgreSQLConfig = TestDataProvider.PostgreSQLConfig;

    DatabaseConfig sqlserverConfig = TestDataProvider.SQLServerConfig.setCatalog("test_db").setSchema("dbo");

//    String tableName = "test_table";
    String tableName = "test_table_clone";
//    String tableName = "lineitem_sf10";
//    String tableName = "customer";
//    String tableName = "testtable";
//    String tableName = "exact_numerics_sqlserver";
    DatabaseConfig config = mysqlConfig;
//    DatabaseConfig config = postgreSQLConfig;
//    DatabaseConfig config = sqlserverConfig;
    @Test
    public void test(){
        showTableInfoBySQL(config,tableName);
        showTableInfoByJdbcMetadata(config,tableName);

        TableCloneManager tcm = TestDataProvider.getTCM(config, config);
        MappingTool tool = MappingToolFactory.create(config.getDataSourceEnum());
        assert tool != null : "Unable support "+config.getDataSourceEnum();
        System.out.println(tool.createSourceMappingTable(tcm.getSourceTableByTableName(config,tableName)).outTableInfo());
    }


    public void showTableInfoBySQL(DatabaseConfig config,String tableName){
//        String sql = String.format("select * from information_schema.COLUMNS where table_name in ('%s')",tableName);
        String sql = "select * from information_schema.COLUMNS where table_name in (?)";
//        String sql = "load data local infile '/usr/local/extended/app/TCM-Temp-mysql/Export_from_MYSQL_lineitem_mysql.csv' into table test fields terminated by '|';";
        try(Connection con = DatasourceConnectionFactory.createDataSourceConnection(config);
            PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1,tableName);
            ResultSet colAllRet = ps.executeQuery();
            StringBuilder sb = new StringBuilder("Show Table Info By Sql Query :\n\n");
            while (colAllRet.next()){
                ResultSetMetaData rsmd = colAllRet.getMetaData();
                int count = rsmd.getColumnCount();
                for (int i = 1;i<=count;i++){
                    String columnName = rsmd.getColumnName(i);
                    sb.append(String.format("%s:%-20s",columnName,colAllRet.getString(columnName)));
                }
                sb.append("\n");
            }
            System.out.println(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * "%" 表示所有任意的（字段）
     */
    public void showTableInfoByJdbcMetadata(DatabaseConfig config,String tableName){
        try(Connection con = DatasourceConnectionFactory.createDataSourceConnection(config)) {
            DatabaseMetaData metaData = con.getMetaData();
//            ResultSet rs = metaData.getTables(null, "%", tableName, new String[]{"TABLE"});
//            while (rs.next())
//                System.out.println("TABLE_NAME:"+rs.getString("TABLE_NAME"));
//            ResultSet colRet = metaData.getColumns(null, "%", tableName, "%");
//            while (colRet.next()){
//                String tableCat = colRet.getString("TABLE_CAT");
//                String tableSchem = colRet.getString("TABLE_SCHEM");
//                String columnName = colRet.getString("COLUMN_NAME");
//                String columnType = colRet.getString("TYPE_NAME");
//                int datasize = colRet.getInt("COLUMN_SIZE");
//                int decimal_digits = colRet.getInt("DECIMAL_DIGITS");
//                int num_prec_radix = colRet.getInt("NUM_PREC_RADIX");
//                System.out.printf("TABLE_CAT:%-20s TABLE_SCHEM:%-20s COLUMN_NAME:%-20s TYPE_NAME:%-20s COLUMN_SIZE:%-10d DECIMAL_DIGITS:%-10d NUM_PREC_RADIX:%-10d\n",tableCat,tableSchem,columnName,columnType,datasize,decimal_digits,num_prec_radix);
//            }
            ResultSet colAllRet = metaData.getColumns(null, "%", tableName, "%");
            StringBuilder sb = new StringBuilder("Show Table Info By JDBC Metadata :\n\n");
            while (colAllRet.next()){
                ResultSetMetaData rsmd = colAllRet.getMetaData();
                int count = rsmd.getColumnCount();
                for (int i = 1;i<=count;i++){
                    String columnName = rsmd.getColumnName(i);
                    sb.append(String.format("%s:%-20s",columnName,colAllRet.getString(columnName)));
                }
                sb.append("\n");
            }
            System.out.println(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *=========================================================== table =======================================
     * https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getTables-java.lang.String-java.lang.String-java.lang.String-java.lang.String:A-
     *
     * TABLE_CAT String => 表类别（可为 null）
     * TABLE_SCHEM String => 表模式（可为 null）
     * TABLE_NAME String => 表名称
     * TABLE_TYPE String => 表类型。典型的类型是 "TABLE"、"VIEW"、"SYSTEM TABLE"、"GLOBAL TEMPORARY"、"LOCAL TEMPORARY"、"ALIAS" 和 "SYNONYM"。
     * REMARKS String => 表的解释性注释
     * TYPE_CAT String => 类型的类别（可为 null）
     * TYPE_SCHEM String => 类型模式（可为 null）
     * TYPE_NAME String => 类型名称（可为 null）
     * SELF_REFERENCING_COL_NAME String => 有类型表的指定 "identifier" 列的名称（可为 null）
     * REF_GENERATION String => 指定在 SELF_REFERENCING_COL_NAME 中创建值的方式。这些值为 "SYSTEM"、"USER" 和 "DERIVED"。（可能为 null）
     *
     *
     *
     *
     *
     * =========================================================== columns =======================================
     * https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getColumns-java.lang.String-java.lang.String-java.lang.String-java.lang.String-
     *
     * TABLE_CAT String => 表类别（可为 null）
     * TABLE_SCHEM String => 表模式（可为 null）
     * TABLE_NAME String => 表名称
     * COLUMN_NAME String => 列名称
     * DATA_TYPE int => 来自 java.sql.Types 的 SQL 类型
     * TYPE_NAME String => 数据源依赖的类型名称，对于 UDT，该类型名称是完全限定的
     * COLUMN_SIZE int => 列的大小。
     * BUFFER_LENGTH 未被使用。
     * DECIMAL_DIGITS int => 小数部分的位数。对于 DECIMAL_DIGITS 不适用的数据类型，则返回 Null。
     * NUM_PREC_RADIX int => 基数（通常为 10 或 2）
     * NULLABLE int => 是否允许使用 NULL。
     * columnNoNulls - 可能不允许使用 NULL 值
     * columnNullable - 明确允许使用 NULL 值
     * columnNullableUnknown - 不知道是否可使用 null
     * REMARKS String => 描述列的注释（可为 null）
     * COLUMN_DEF String => 该列的默认值，当值在单引号内时应被解释为一个字符串（可为 null）
     * SQL_DATA_TYPE int => 未使用
     * SQL_DATETIME_SUB int => 未使用
     * CHAR_OCTET_LENGTH int => 对于 char 类型，该长度是列中的最大字节数
     * ORDINAL_POSITION int => 表中的列的索引（从 1 开始）
     * IS_NULLABLE String => ISO 规则用于确定列是否包括 null。
     * YES --- 如果参数可以包括 NULL
     * NO --- 如果参数不可以包括 NULL
     * 空字符串 --- 如果不知道参数是否可以包括 null
     * SCOPE_CATLOG String => 表的类别，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
     * SCOPE_SCHEMA String => 表的模式，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
     * SCOPE_TABLE String => 表名称，它是引用属性的作用域（如果 DATA_TYPE 不是 REF，则为 null）
     * SOURCE_DATA_TYPE short => 不同类型或用户生成 Ref 类型、来自 java.sql.Types 的 SQL 类型的源类型（如果 DATA_TYPE 不是 DISTINCT 或用户生成的 REF，则为 null）
     * IS_AUTOINCREMENT String => 指示此列是否自动增加
     * YES --- 如果该列自动增加
     * NO --- 如果该列不自动增加
     * 空字符串 --- 如果不能确定该列是否是自动增加参数
     * COLUMN_SIZE 列表示给定列的指定列大小。对于数值数据，这是最大精度。对于字符数据，这是字符长度。对于日期时间数据类型，这是 String 表示形式的字符长度（假定允许的最大小数秒组件的精度）。对于二进制数据，这是字节长度。对于 ROWID 数据类型，这是字节长度。对于列大小不适用的数据类型，则返回 Null。
     */


    @Test
    void name() {
        DatabaseConfig pg = TestDataProvider.PostgreSQLConfig;
//        String pg_tablename = "lineitem_sf10";
        String pg_tablename = "t1";
        String sql = "select * from information_schema.COLUMNS where table_name in (?)";

        try(Connection con = DatasourceConnectionFactory.createDataSourceConnection(pg);
            PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1,pg_tablename);
            ResultSet colAllRet = ps.executeQuery();
            StringBuilder sb = new StringBuilder("Show Table Info By Sql Query :\n\n");
            while (colAllRet.next()){
                ResultSetMetaData rsmd = colAllRet.getMetaData();
                int count = rsmd.getColumnCount();
                for (int i = 1;i<=count;i++){
                    String columnName = rsmd.getColumnName(i);
                    sb.append(String.format("%s:%-20s",columnName,colAllRet.getString(columnName)));
                }
                sb.append("\n");
            }
            System.out.println(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
