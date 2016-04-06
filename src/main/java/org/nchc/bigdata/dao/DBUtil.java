package org.nchc.bigdata.dao;

import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class DBUtil {
    private static Logger logger = Logger.getLogger(DBUtil.class);
    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(Util.traceString(e));
            }
        }
    }

    public static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error(Util.traceString(e));
            }
        }
    }

    public static void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error(Util.traceString(e));
            }
        }
    }

    /**
     * Check the connection status.
     *
     * @param connection
     *            the connection to check
     * @return if the connection is ready
     */
    public static boolean checkConnAndCloseIfUnvalid(Connection connection){
        if (connection == null)
            return false;

        try{
            if(!connection.isValid(1)) {
                return false;
            }
        }catch(SQLException e) {
            logger.error(Util.traceString(e));
        }

        return true;
    }
}
