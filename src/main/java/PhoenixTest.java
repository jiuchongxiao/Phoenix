//package com.tiza.phoenix.test;

import java.sql.*;

/**
 * Created by tz627 on 2016/9/9.
 */
public class PhoenixTest {
    // after 4.3 version
    private static final String CLASS_FORNAME = "org.apache.phoenix.jdbc.PhoenixDriver";
    // single or many
    // config a right hostname of the zookeeper cluster,it will work
    private static final String URL = "jdbc:phoenix:M6vm-3-42";

    public static void main(String[] args) throws Exception{
//        queryRowCount();
        
        insertData();
        queryEveryRowRecord();
        /*Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection("jdbc:phoenix:M6vm-3-42,M6vm-3-41","root","root");
        stmt = conn.createStatement();
        stmt.execute("upsert into \"blog6\" values ('111', 'note of huhong')");*/

    }


    public static void insertData(){
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName(CLASS_FORNAME);
            conn = DriverManager.getConnection(URL);
            stmt = conn.createStatement();
//            stmt.execute("create table blog7(\"ROW\" varchar primary key,\"article\".\"title\" varchar,\"article\".\"content\" varchar,\"article\".\"tag\" varchar,\"author\".\"name\" varchar,\"author\".\"nickname\" varchar)");
            stmt.execute("upsert into \"blog9\" values ('6','wang5','wang_tag5')");
            stmt.execute("upsert into \"blog9\" values ('7','wang5','wang_tag7')");
            stmt.execute("upsert into \"blog9\" values ('8','wang5','wang_tag8')");




//            stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
//            stmt.executeUpdate("upsert into test values (1,'Hello2')");
//            stmt.executeUpdate("upsert into test values (2,'World!2')");
//            stmt.executeUpdate("upsert into test values (3,'World!3')");
            conn.commit();
            ResultSet rset = null;
            PreparedStatement statement = conn.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            close(conn, stmt, rs);
        }
    }


    public static void queryRowCount() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {

            Class.forName(CLASS_FORNAME);
            conn = DriverManager.getConnection(URL);
            stmt = conn.createStatement();
            String sql = "select count(1) as num from blog6";
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
            String sql = "select * from \"blog9\"";
            long time = System.currentTimeMillis();
            rs = stmt.executeQuery(sql);
            /*while (rs.next()) {
                String title = rs.getString("title");
                String content = rs.getString("content");
                String tag = rs.getString("tag");
                String name = rs.getString("name");
                String nickname = rs.getString("nickname");
                System.out.println("title:"+title+" content:"+content+" tag:"+tag+" name:"+name+" nickname:"+nickname);
            }*/
            while (rs.next()) {
                String tag = rs.getString("tag");
                String content = rs.getString("content");

                System.out.println("tag:"+tag+" content:"+content);
            }
            long timeUsed = System.currentTimeMillis() - time;
            System.out.println("time " + timeUsed + " mm");

            /*Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:M6vm-3-42,M6vm-3-41","root","root");
            stmt = conn.createStatement();
            stmt.execute("upsert into 'blog6' values (3, 'note of huhong')");*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn, stmt, rs);
        }
    }

    private static void close(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if(null!=rs)
            rs.close();
            if(null!=stmt)
            stmt.close();
            if(null!=conn)
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
