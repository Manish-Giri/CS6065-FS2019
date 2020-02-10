package com.manishgiri.cloud.JDBCSQLTest;

import javax.swing.plaf.nimbus.State;
import java.sql.*;

public class SQLTest {

    private final String dbURL = "jdbc:mysql://localhost:3306/ebookshop?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC";
    private final String user = "mysqluser";
    private final String password = "password";

    private static void select(Connection conn, Statement stmt) {
        try {

            // Step 3: Execute a SQL SELECT query. The query result is returned in a 'ResultSet' object.
            String strSelect = "select title, price, qty from books";
            System.out.println("The SQL statement is: " + strSelect + "\n"); // Echo For debugging

            ResultSet rset = stmt.executeQuery(strSelect);

            // Step 4: Process the ResultSet by scrolling the cursor forward via next().
            //  For each row, retrieve the contents of the cells with getXxx(columnName).
            System.out.println("The records selected are:");
            int rowCount = 0;
            while(rset.next()) {   // Move the cursor to the next row, return false if no more row
                String title = rset.getString("title");
                double price = rset.getDouble("price");
                int    qty   = rset.getInt("qty");
                System.out.println(title + ", " + price + ", " + qty);
                ++rowCount;
            }
            System.out.println("Total number of records = " + rowCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void update(Connection conn, Statement stmt, String query) {
        try {
            System.out.println("Query: " + query);
            int resultCount = stmt.executeUpdate(query);
            System.out.println("Number of rows affected: " + resultCount);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        try (
                // Step 1: Allocate a database 'Connection' object
                Connection conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/ebookshop?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC",
                        "mysqluser", "password");   // For MySQL only
                // The format is: "jdbc:mysql://hostname:port/databaseName", "username", "password"

                // Step 2: Allocate a 'Statement' object in the Connection
                Statement stmt = conn.createStatement();
        ) {
            // Step 3: Execute a SQL SELECT query. The query result is returned in a 'ResultSet' object.
            //select(conn, stmt);

            // 4. execute UPDATE
            String updateQuery = "UPDATE books set price = price * 0.5, qty = qty + 1 where id = 1002";
            //update(conn, stmt, updateQuery);

            // 5. execute INSERT ONE RECORD
            String insertOneQuery = "INSERT INTO books values (3001, 'Lord of the Rings', 'J.R.R Tolkien', 300, 66)";


            String insertMultipleQuery = "INSERT INTO books values (4001, 'Harry Potter', 'J.K Rowling', 300, 30), " +
                    "(5001, 'Game of thrones', 'Jon Snow', 3400, 5)";
            update(conn, stmt, insertMultipleQuery);

        } catch(SQLException ex) {
            ex.printStackTrace();
        }  // Step 5: Close conn and stmt - Done automatically by try-with-resources (JDK 7)
    }
}
