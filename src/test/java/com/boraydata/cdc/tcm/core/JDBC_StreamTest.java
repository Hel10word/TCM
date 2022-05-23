package com.boraydata.cdc.tcm.core;

import com.boraydata.cdc.tcm.TestDataProvider;
import com.boraydata.cdc.tcm.common.DatabaseConfig;
import com.boraydata.cdc.tcm.common.DatasourceConnectionFactory;
import com.boraydata.cdc.tcm.common.enums.DataSourceEnum;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author bufan
 * @data 2021/10/14
 */
public class JDBC_StreamTest {

    //========================== MySQL ===============================
    DatabaseConfig configMySQL = TestDataProvider.MySQLConfig;

    //========================== PgSQL ===============================
    DatabaseConfig configPGSQL = TestDataProvider.PostgreSQLConfig;

    private String sql = "select * from lineitem_sf1_pgsql limit 10000;";

    Connection mysqlCon = DatasourceConnectionFactory.createDataSourceConnection(TestDataProvider.MySQLConfig);
    Connection pgsqlCon = DatasourceConnectionFactory.createDataSourceConnection(configPGSQL);

    @Test
    public void foo(){
        long start = System.currentTimeMillis();
        try{
            PreparedStatement stmt = pgsqlCon.prepareStatement(sql);
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                insert(mysqlCon,new String[]{
                        //  0  l_orderkey    integer
                        String.valueOf(rs.getInt("l_orderkey")),
                        //  1  L_PARTKEY     INTEGER
                        String.valueOf(rs.getInt("L_PARTKEY")),
                        //  2  L_SUPPKEY     INTEGER
                        String.valueOf(rs.getInt("L_SUPPKEY")),
                        //  3  L_LINENUMBER  INTEGER
                        String.valueOf(rs.getInt("L_LINENUMBER")),
                        //  4  L_QUANTITY    DECIMAL(15,2)
                        String.valueOf(rs.getDouble("L_QUANTITY")),
                        //  5  L_EXTENDEDPRICE  DECIMAL(15,2)
                        String.valueOf(rs.getDouble("L_EXTENDEDPRICE")),
                        //  6  L_DISCOUNT    DECIMAL(15,2)
                        String.valueOf(rs.getDouble("L_DISCOUNT")),
                        //  7  L_TAX         DECIMAL(15,2)
                        String.valueOf(rs.getDouble("L_TAX")),
                        //  8  L_RETURNFLAG  CHAR(1)
                        String.valueOf(rs.getString("L_RETURNFLAG")),
                        //  9  L_LINESTATUS  CHAR(1)
                        String.valueOf(rs.getString("L_LINESTATUS")),
                        //  10  L_SHIPDATE    DATE
                        String.valueOf(rs.getDate("L_SHIPDATE")),
                        //  11  L_COMMITDATE  DATE
                        String.valueOf(rs.getDate("L_COMMITDATE")),
                        //  12  L_RECEIPTDATE DATE
                        String.valueOf(rs.getDate("L_RECEIPTDATE")),
                        //  13  L_SHIPINSTRUCT CHAR(25)
                        String.valueOf(rs.getString("L_SHIPINSTRUCT")),
                        //  14  L_SHIPMODE     CHAR(10)
                        String.valueOf(rs.getString("L_SHIPMODE")),
                        //  15  L_COMMENT      VARCHAR(44)
                        String.valueOf(rs.getString("L_COMMENT")),
                });
//                System.out.println("One:"+rs.getString(1));
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("\t---- total time spent:" + (end - start)+"\n");
    }

    @Test
    public void insert(Connection con, String[] args){

//        INSERT INTO "lineitem_pgsql" VALUES (1701, 53004, 3005, 2, '2.00', '1914.00', '0.01', '0.04', 'R', 'F', '1992-06-24', '1992-07-12', '1992-06-29', 'COLLECT COD              ', 'SHIP      ', 'ween the pending, final accounts. ');
        String insStr = "INSERT INTO lineitem_sf1_pgsql VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        try{
            PreparedStatement stmt = con.prepareStatement(insStr);
            stmt.setFetchSize(1000);
            for (int i = 0 ;i<16;i++)
                stmt.setString(i+1,args[i]);
//            System.out.println(stmt.toString());
            int i = stmt.executeUpdate();
//            System.out.println(i);
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }




}
