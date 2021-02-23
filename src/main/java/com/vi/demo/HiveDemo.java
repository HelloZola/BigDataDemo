package com.vi.demo;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveDemo {

    private static Connection conn = null;
    private static Connection connToMysql = null;
    //    private static String JDBC_URL = "jdbc:hive2://10.58.11.211:10000/default;password=root;user=root;";
    private static String JDBC_URL = "jdbc:hive2://127.0.0.1:10000/default;";

    public HiveDemo() {
    }

    // 获得hive连接
    public static Connection getHiveConn() throws SQLException {
        if (conn == null) {
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            conn = DriverManager.getConnection(JDBC_URL);
        }
        return conn;
    }

    // 获得hive连接
    public static Connection getHiveConn(Properties info) throws SQLException {
        if (conn == null) {
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            conn = DriverManager.getConnection(JDBC_URL, info);
        }
        return conn;
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
        Connection conn = HiveDemo.getHiveConn();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(hql);
        return rs;
    }

    @Test
    public void testCreateTable() {

        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
//            stmt.execute("create table datetable2(words string) partitioned by (partdate date) STORED AS PARQUET ;");
            stmt.execute("create table student(id int, name string) STORED AS PARQUET");
        } catch (SQLException throwables) {
            System.out.println("test");
            throwables.printStackTrace();
        }
    }

    @Test
    public void testInsertTable() {

        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
            stmt.execute("INSERT INTO TABLE student select 10,'chen'");
        } catch (SQLException throwables) {
            System.out.println("test");
            throwables.printStackTrace();
        }
    }

    @Test
    public void testReport() {

        try {
            Properties info = new Properties();
            info.setProperty("hiveconf:hive.execution.engine", "tez");
            info.setProperty("tez.application.tags", "hive-client-job-test-" + System.currentTimeMillis());
            info.setProperty("mapred.job.name", "hive2-client-job-test-" + System.currentTimeMillis());
            info.setProperty("hiveconf:hive.session.id", "hive3-client-job-test-" + System.currentTimeMillis());
            info.setProperty("hive.session.id", "hive3-client-job-test-" + System.currentTimeMillis());
//            info.setProperty("mr.application.tags", "hive-client-job-test-" + System.currentTimeMillis());
            Connection conn = HiveDemo.getHiveConn(info);
            Statement stmt = conn.createStatement();
            System.out.println("开始插入数据");
            stmt.execute("INSERT INTO TABLE student select 10,'chen'");
            System.out.println("结束插入数据");
            Set<String> applicationTypes = Sets.newHashSet();
//            applicationTypes.add("TEZ");
//            applicationTags.add("hive-client-job-test");

            Set<YarnApplicationState> applicationStates = Sets.newHashSet();
            EnumSet<YarnApplicationState> enumStates = Sets.newEnumSet(applicationStates, YarnApplicationState.class);
            System.out.println("增加单子筛选状态");
            enumStates.add(YarnApplicationState.RUNNING);
            enumStates.add(YarnApplicationState.ACCEPTED);
            enumStates.add(YarnApplicationState.SUBMITTED);
            enumStates.add(YarnApplicationState.FINISHED);
            enumStates.add(YarnApplicationState.NEW);
            enumStates.add(YarnApplicationState.NEW_SAVING);
            enumStates.add(YarnApplicationState.FAILED);
            enumStates.add(YarnApplicationState.KILLED);

            YarnClient client = YarnClient.createYarnClient();
            Configuration conf = new Configuration();
            // conf.set();
            client.init(conf);
            client.start();

            List<ApplicationReport> reports = client.getApplications(applicationTypes, enumStates);
            System.out.println("reports size:" + reports.size());
            for (ApplicationReport report : reports) {
                System.out.println(report.getName());
                System.out.println("name:" + report.getName() + "  applicationId:" + report.getApplicationId() + "   status:" + report.getYarnApplicationState() + "  tags:" + report.getApplicationTags());
            }
            System.out.println("end");
        } catch (Exception throwables) {
            System.out.println("test");
            throwables.printStackTrace();
        }
    }

    @Test
    public void getTableLocation() {

        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("show create table datetable2");
            String desc = "";
            boolean nextIsLocation = false;
            while (rs.next()) {
                desc = rs.getString(1);

                if (nextIsLocation) {
                    break;
                }

                if (desc.equalsIgnoreCase("LOCATION")) {
                    nextIsLocation = true;
                }
            }
            System.out.println(desc);

            String rgex2 = "([\\w\\W]*)(\')([\\w\\W]*)(\')";
            Pattern pattern2 = Pattern.compile(rgex2);// 匹配的模式
            Matcher m2 = pattern2.matcher(desc);
            String location = "";
            while (m2.find()) {
                System.out.println("----------------------------");
                System.out.println(m2.group(0));
                System.out.println("----------------------------");
                System.out.println(m2.group(1));
                System.out.println("----------------------------");
                System.out.println(m2.group(2));
                System.out.println("----------------------------");
                System.out.println(m2.group(3));
                location = m2.group(3);
                System.out.println("----------------------------");
                System.out.println(m2.group(4));
                System.out.println("----------------------------");
            }

            String[] srt = location.split("\\/");
            System.out.println(srt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQuery() {
        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
//            stmt.execute("use default");
//            stmt.execute("show tblproperties DEFAULT.test_465jdsjsdf tablename ('rawDataSize')");
//            ResultSet rs = stmt.executeQuery("show tblproperties DEFAULT.flattable1612268575436 ('rawDataSize')");
//            ResultSet rs = stmt.executeQuery("desc extended test_465jdsjsdf");
            ResultSet rs = stmt.executeQuery("show create table student");
            String desc = "";
            while (rs.next()) {
                desc = rs.getString(1);
                System.out.println(desc);
            }

            String rgex2 = "(CREATE)([\\w\\W]*)(LOCATION)([\\w\\W]*)(TBLPROPERTIES)";
            Pattern pattern2 = Pattern.compile(rgex2);// 匹配的模式
            Matcher m2 = pattern2.matcher(desc);
            while (m2.find()) {
                System.out.println("----------------------------");
                System.out.println(m2.group(0));
                System.out.println("----------------------------");
                System.out.println(m2.group(1));
                System.out.println("----------------------------");
                System.out.println(m2.group(2));
                System.out.println("----------------------------");
                System.out.println(m2.group(3));
                System.out.println("----------------------------");
                System.out.println(m2.group(4));
                System.out.println("----------------------------");
                System.out.println(m2.group(5));
                System.out.println("----------------------------");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQuery2() {
        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from flatTable1612274199625");
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
                System.out.println(rs.getString(4));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQuerySize() {
        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
            //analyze table  datetable2  [partition(partCol[='value'])] compute statistics;
            ResultSet rs = stmt.executeQuery("analyze table datetable2 partition(partdate='2018-12-27') compute statistics");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testShowAllTables() {
        try {
            Connection conn = HiveDemo.getHiveConn();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("show tables");

            while (rs.next()) {
                String tables = rs.getString(1);
                System.out.println(tables);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}