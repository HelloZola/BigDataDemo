package com.vi.demo;

import org.junit.Test;

import java.sql.*;

public class HiveDemo {


    private static String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    //    private static String URL = "jdbc:hive2://127.0.0.1:10000/default";
    private static String URL = "jdbc:hive2://localhost:10000/default";
    private static String USERNAME = "";
    private static String PASSWORD = "";

    private static Connection connection;
    private static Statement statement;

    static {
        try {
            // 加载hive jdbc驱动
            Class.forName(DRIVER);
            // 获取连接
//            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            connection = DriverManager.getConnection(URL);
            // 获取statement
            statement = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void select() {
        try {
            String sql = "select * from datetable1";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
                System.out.println(resultSet.getDate(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void insert() {
        try {
            long start = System.currentTimeMillis();
//            String sql = "INSERT INTO TABLE datetable1 select 'chen2',cast('2018-11-30' as date)";
            String sql = "insert into datetable2 PARTITION (partdate) select demensiontable.name,nodatetable3.partdate2  from nodatetable3 " +
                    "left join demensiontable on nodatetable3.code = demensiontable.code2";
            boolean resultSet = statement.execute(sql);
            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void showTables() {
        try {
            String sql = "show tables";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * drop table if exists iot_devicelocation;
     * ALTER TABLE schedule_events drop if exists partition (year>'0');
     */
    @Test
    public void dropTable() {
        try {
            String sql = "drop table if exists " + "";
            boolean resultSet = statement.execute(sql);
            System.out.println(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
