package com.vi.demo;

import java.sql.*;

public class HiveDemoTest {

    private static Connection conn = null;
    private static Connection connToMysql = null;

    private HiveDemoTest() {
    }

    // 获得hive连接
    public static Connection GetHiveConn() throws SQLException {
        if (conn == null) {
            try {
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            conn = DriverManager.getConnection(
                    "jdbc:hive://127.0.0.1:50031/default", "", "");
        }
        return conn;
    }

    // 获得sql连接
    public static Connection getMySqlConn() throws SQLException {
        if (connToMysql == null) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {

                e.printStackTrace();
                System.exit(1);
            }
            connToMysql = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/hive", "junjun", "123456");
        }
        return connToMysql;
    }

    public static void closeHive() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    public static void closeMysql() throws SQLException {
        if (connToMysql != null) {
            connToMysql.close();
        }
    }

    public static ResultSet queryHive(String hql) throws SQLException {
        Connection conn = HiveDemoTest.GetHiveConn();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(hql);
        return rs;
    }

    public static void main(String[] args) {
        try {
            HiveDemoTest.queryHive("select * from default.student");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
