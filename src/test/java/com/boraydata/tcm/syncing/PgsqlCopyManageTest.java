package com.boraydata.tcm.syncing;

import com.boraydata.tcm.TestDataProvider;
import com.boraydata.tcm.configuration.DatabaseConfig;
import com.boraydata.tcm.configuration.DatasourceConnectionFactory;
import com.boraydata.tcm.core.DataSourceType;
import com.boraydata.tcm.core.TableCloneManageContext;
import com.boraydata.tcm.entity.Table;
import com.boraydata.tcm.utils.FileUtil;
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
 * @data 2022/3/30
 */
public class PgsqlCopyManageTest {

    PgsqlSyncingTool tool = new PgsqlSyncingTool();



    // unable Export or Load Client CSV File in PgSQL Server
    // Exception: org.postgresql.util.PSQLException: ERROR: relative path not allowed for COPY to file
    // https://www.postgresql.org/message-id/CFF47E56EA077241B1FFF390344B5FC10ACB1C0C@webmail.begavalley.nsw.gov.au
    @Test
    public void JdbcDriver(){
        DatabaseConfig.Builder pgsql = new DatabaseConfig.Builder();
        DatabaseConfig pgsqlConfig = pgsql
                .setDatabasename("test_db")
                .setDataSourceType(DataSourceType.POSTGRES)
                .setHost("192.168.30.31")
                .setPort("5432")
                .setUsername("postgres")
                .setPassword("postgres")
                .create();
//        String filePath = "E:/Desktop/test.csv";
        String filePath = "usr/test.csv";
        String tableName = "lineitem_test";
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(pgsqlConfig);
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
        DatabaseConfig.Builder pgsql = new DatabaseConfig.Builder();
        DatabaseConfig pgsqlConfig = pgsql
                .setDatabasename("sf10")
                .setDataSourceType(DataSourceType.POSTGRES)
                .setHost("192.168.120.73")
                .setPort("5432")
                .setUsername("postgres")
                .setPassword("rdpadmin")
                .create();
        String filePath = "E:\\Desktop\\test.csv";
        String tableName = "lineitem_test";
        File file = FileUtil.createNewFile(filePath);
        try(
                Connection conn = DatasourceConnectionFactory.createDataSourceConnection(pgsqlConfig);
                FileOutputStream fileOutputStream = new FileOutputStream(file)
        ){
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            long l = copyManager.copyOut("COPY " + tableName + " TO STDIN WITH DELIMITER ','", fileOutputStream);
            System.out.println("ExportDataToPgSQLByJDBC: "+l);
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
        TableCloneManageContext tcmc = TestDataProvider.getTCMC(TestDataProvider.getConfigPGSQL(), TestDataProvider.getConfigPGSQL());
        Table table = TestDataProvider.getTable(DataSourceType.MYSQL, "lineitem_test");
        tcmc.setSourceTable(table)
//                .setTempTable(table)
                .setCloneTable(table)
                .setCsvFileName("test.csv");

//        tool

    }
}
