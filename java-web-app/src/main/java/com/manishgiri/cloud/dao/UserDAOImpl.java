package com.manishgiri.cloud.dao;

import com.manishgiri.cloud.DBConnection;
import com.manishgiri.cloud.dao.UserDAO;
import com.manishgiri.cloud.model.User;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UserDAOImpl implements UserDAO {
    @Override
    public boolean addUser(String username, String password) {

        Integer rowsUpdated = DBConnection.insertUsername(username, password);
        return rowsUpdated != null && rowsUpdated != 0;
    }

    @Override
    public User findUser(String username, String password) {

        return DBConnection.fetchUser(username, password);
    }

    @Override
    public boolean addUserDetails(String f, String l, String e, String u) {
        Integer rowsUpdated = DBConnection.insertUserDetails(f, l, e, u);
        return rowsUpdated != null && rowsUpdated != 0;
    }
}
