package com.boraydata.cdc.tcm.syncing;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import com.boraydata.cdc.tcm.core.TableCloneManagerContext;
import com.boraydata.cdc.tcm.syncing.tool.Mysql;
import com.boraydata.cdc.tcm.syncing.tool.PostgreSQL;
import com.boraydata.cdc.tcm.utils.FileUtil;
import com.boraydata.cdc.tcm.entity.Table;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author bufan
 * @date 2022/3/30
 */
public class PostgreSQLSyncingToolTest {

    PostgreSQLSyncingTool tool = new PostgreSQLSyncingTool();



    // unable Export or Load Client CSV File in PostgreSQL Server
    // Exception: org.postgresql.util.PSQLException: ERROR: relative path not allowed for COPY to file
    // https://www.postgresql.org/message-id/CFF47E56EA077241B1FFF390344B5FC10ACB1C0C@webmail.begavalley.nsw.gov.au
    @Test
    public void JdbcDriver(){
        DatabaseConfig postgreSQLConfig = TestDataProvider.PostgreSQLConfig;
//        String filePath = "E:/Desktop/test.csv";
        String filePath = "usr/test.csv";
        String tableName = "lineitem_test";
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(postgreSQLConfig);
                Statement statement = conn.createStatement();
        ){
            boolean execute = statement.execute("copy " + tableName + " to '" + filePath + "' with DELIMITER ',';");
            System.out.println(execute);

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public static void CopyManagerTest() {
        DatabaseConfig postgreSQLConfig = TestDataProvider.PostgreSQLConfig;
        String filePath = "E:\\Desktop\\test.csv";
        String tableName = "lineitem_test";
        File file = FileUtil.createNewFile(filePath);
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(postgreSQLConfig);
                FileOutputStream fileOutputStream = new FileOutputStream(file)
        ){
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            long l = copyManager.copyOut("COPY " + tableName + " TO STDIN WITH DELIMITER ','", fileOutputStream);
            System.out.println("ExportDataToPostgreSQLByJDBC: "+l);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void JdbcDriverCopyManagerTest() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String url = "jdbc:postgresql://192.168.30.31:5432/test_db";
        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","root");
        Driver driver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
        try {
            try(
                    Connection conn = driver.connect(url,properties);
                    FileOutputStream file = new FileOutputStream("E:\\Desktop\\lineitem.csv")
                    ){
                CopyManager copyManager = new CopyManager((BaseConnection) conn);
                long l = copyManager.copyOut("COPY lineitem_test TO STDIN WITH DELIMITER ','", file);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getSQLShellTest(){
        PostgreSQL tool = new PostgreSQL();
        DatabaseConfig config = new DatabaseConfig()
                .setDataSourceEnum(DataSourceEnum.POSTGRESQL)
                .setHost("192.168.10.11")
                .setPort("6432")
                .setUsername("postgres")
                .setPassword("123456")
                .setDatabaseName("test_db")
                .setCatalog("test_db")
                .setSchema("public");
        TableCloneManagerContext tcmc = TestDataProvider.getTCMContext(config, config);
        Table table = TestDataProvider.getTable(DataSourceEnum.POSTGRESQL, "lineitem_test");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");
        tcmc.getTcmConfig()
                .setDelimiter("|")
                .setLineSeparate(null)
                .setQuote("\"")
                .setEscape("\\");

        System.out.println("generateExportSQLByShell:\n"+tool.generateExportSQLByShell(tcmc)+"\n");
        System.out.println("generateLoadSQLByShell:\n"+tool.generateLoadSQLByShell(tcmc));

    }
}
