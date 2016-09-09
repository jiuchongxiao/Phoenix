package com.tiza.phoenix.test;

import java.sql.*;

/**
 * Created by tz627 on 2016/9/9.
 */
public class PhoenixTest {
    // after 4.3 version
    private static final String CLASS_FORNAME = "org.apache.phoenix.jdbc.PhoenixDriver";
    // single or many
    // config a right hostname of the zookeeper cluster,it will work
    private static final String URL = "jdbc:phoenix:tzcdh100,tzcdh101,tzcdh102";

    public static void main(String[] args) {
//        queryRowCount();
        queryEveryRowRecord();

    }

    public static void queryRowCount() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {

            Class.forName(CLASS_FORNAME);
            conn = DriverManager.getConnection(URL);
            stmt = conn.createStatement();
            String sql = "select count(1) as num from web_stat";
            long time = System.currentTimeMillis();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int count = rs.getInt("num");
                System.out.println("row count is " + count);
            }
            long timeUsed = System.currentTimeMillis() - time;
            System.out.println("time " + timeUsed + " mm");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn, stmt, rs);
        }
    }

    public static void queryEveryRowRecord() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName(CLASS_FORNAME);
            conn = DriverManager.getConnection(URL);
            stmt = conn.createStatement();
            String sql = "select * from web_stat";
            long time = System.currentTimeMillis();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int core = rs.getInt("core");
                String host = rs.getString("host");
                String domain = rs.getString("domain");
                String feature = rs.getString("feature");
                String date = rs.getString("date");
                String db = rs.getString("db");
                System.out.println("core:" + core + ",host:" + host + ",domain:" + domain + ",feature:" + feature + ",date:" + date + ",db:" + db);
            }
            long timeUsed = System.currentTimeMillis() - time;
            System.out.println("time " + timeUsed + " mm");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn, stmt, rs);
        }
    }

    private static void close(Connection conn, Statement stmt, ResultSet rs) {
        try {
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
