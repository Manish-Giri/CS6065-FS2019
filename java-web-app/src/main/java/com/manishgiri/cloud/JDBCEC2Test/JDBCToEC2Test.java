package com.manishgiri.cloud.JDBCEC2Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCToEC2Test {

    private static final String PUBLIC_DNS = "ec2-18-224-184-130.us-east-2.compute.amazonaws.com";
    private static final String PORT = "3306" ;
    private static final String DATABASE = "dbTest";
    private static final String REMOTE_DATABASE_USERNAME = "remoteu";
    private static final String DATABASE_USER_PASSWORD = "password";

    public static void main(String[] args) {
        System.out.println("----MySQL JDBC Connection Testing -------");

        try(Connection connection = DriverManager.
                getConnection("jdbc:mysql://" + PUBLIC_DNS + ":" + PORT + "/" + DATABASE, REMOTE_DATABASE_USERNAME, DATABASE_USER_PASSWORD)) {
            if (connection != null) {
                System.out.println("SUCCESS!!!! You made it, take control of your database now!");
            } else {
                System.out.println("FAILURE! Failed to make connection!");
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
