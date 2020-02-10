package com.manishgiri.cloud;

import com.manishgiri.cloud.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBConnection {

    // ----------------- LOCAL MYSQL --------------
 /*   private static final String dbURL = "jdbc:mysql://localhost:3306/CCHW1?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC";
    private static final String MYSQLUSER = "mysqluser";
    private static final String MYSQLPASSWORD = "password";*/

    // ------------------- MYSQL on EC2 CREDENTIALS -------------
    private static final String PUBLIC_DNS = "ec2-3-16-11-101.us-east-2.compute.amazonaws.com";
    private static final String PORT = "3306" ;
    private static final String DATABASE = "dbTest";
    private static final String REMOTE_DATABASE_USERNAME = "remoteu";
    private static final String DATABASE_USER_PASSWORD = "password";
    private static final String dbURL = "jdbc:mysql://" + PUBLIC_DNS + ":" + PORT + "/" + DATABASE;


    private static List<User> users = new ArrayList<>();


    public static User fetchUser(String username, String p) {

        String query = "SELECT * from users where username = ? and pass = ?";
        try(Connection conn = DriverManager.getConnection(dbURL, REMOTE_DATABASE_USERNAME, DATABASE_USER_PASSWORD); PreparedStatement ps = conn.prepareStatement(query)) {

            System.out.println("The SQL statement is: " + query);
            ps.setString(1, username);
            ps.setString(2, p);

            ResultSet rset = ps.executeQuery();

            while (rset.next()) {
                String uname = rset.getString("username");

                // populate ARRAYLIST with objects if size is 0
                if(users.size() == 0) {
                    String pass = rset.getString("pass");
                    String firstName = rset.getString("firstname");
                    String lastName = rset.getString("lastname");
                    String email = rset.getString("email");
                    users.add(new User(uname, pass, firstName, lastName, email));
                    return users.get(0);

                }
                else {
                    for(User u: users) {
                        if(u.getUsername().equals(uname)) {
                            return u;
                        }
                    }
                }




                // add obj to arraylist
            }

            return null;


            // Step 4: Process the ResultSet by scrolling the cursor forward via next().
            //  For each row, retrieve the contents of the cells with getXxx(columnName).
            /*System.out.println("The records selected are:");
            int rowCount = 0;
            while(rset.next()) {   // Move the cursor to the next row, return false if no more row
                String title = rset.getString("title");
                double price = rset.getDouble("price");
                int    qty   = rset.getInt("qty");
                System.out.println(title + ", " + price + ", " + qty);
                ++rowCount;
            }
            System.out.println("Total number of records = " + rowCount);*/
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static Integer insertUsername(String newUser, String password) {
        //String newUser = u.getUsername();
        //String password = u.getPassWord();
        String query = "INSERT into users (username, pass) values (?, ?)";
        try (Connection conn = DriverManager.getConnection(dbURL, REMOTE_DATABASE_USERNAME, DATABASE_USER_PASSWORD); PreparedStatement ps = conn.prepareStatement(query)) {
            System.out.println("Query: " + query);
            ps.setString(1, newUser);
            ps.setString(2, password);
            int resultCount = ps.executeUpdate();
            users.add(new User(newUser, password));
            System.out.println("Number of rows affected: " + resultCount);
            return resultCount;
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Integer insertUserDetails(String first, String last, String email, String uname) {
        String query = "UPDATE users SET firstname = ?, lastname = ?, email = ? WHERE username = ?";
        try (Connection conn = DriverManager.getConnection(dbURL, REMOTE_DATABASE_USERNAME, DATABASE_USER_PASSWORD); PreparedStatement ps = conn.prepareStatement(query)) {
            System.out.println("Query: " + query);
            ps.setString(1, first);
            ps.setString(2, last);
            ps.setString(3, email);
            ps.setString(4, uname);
            int resultCount = ps.executeUpdate();

            // update user in DB
            User user = users.stream().filter(u -> u.getUsername().equals(uname)).findFirst().orElse(null);
            if(user != null) {
                user.setFirstName(first);
                user.setLastName(last);
                user.setEmail(email);
                System.out.println("Number of rows affected: " + resultCount);
                return resultCount;
            }
           else {
               return null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }
}
