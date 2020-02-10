package com.manishgiri.cloud.dao;

import com.manishgiri.cloud.model.User;

import java.sql.ResultSet;

public interface UserDAO {

    boolean addUser(String username, String password);
    User findUser(String username, String password);
    boolean addUserDetails(String f, String l, String e, String u);

}
