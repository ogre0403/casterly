package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/18.
 */
public class ConnectionFactory {
    //static reference to itself
    private static Logger logger = Logger.getLogger(ConnectionFactory.class);
    private static ConnectionFactory instance = new ConnectionFactory();
//    public static final String URL = "jdbc:mysql://localhost/jdbcdb";
//    public static final String USER = "YOUR_DATABASE_USERNAME";
//    public static final String PASSWORD = "YOUR_DATABASE_PASSWORD";
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    //private constructor
    private ConnectionFactory() {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Connection createConnection(String url, String user, String password) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Unable to Connect to Database. [" + url + " / " +user + " / " +password +"]");
        }
        return connection;
    }

    public static Connection getConnection(Configuration conf) {
        String url = conf.get("SQL_URL");
        String user = conf.get("SQL_USER");
        String password = conf.get("SQL_PASSWORD");
        return instance.createConnection(url, user,password);
    }
}
