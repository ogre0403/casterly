package org.nchc.bigdata.db;

import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class TaskDAO {

    private Connection connection;
    private Statement statement;
    protected static Configuration conf;

    public void writeTask() throws SQLException {
        String query = "INSERT INTO ...";
        ResultSet rs = null;
        try {
            connection = ConnectionFactory.getConnection(conf);
            statement = connection.createStatement();
            rs = statement.executeQuery(query);
            /*....*/

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
    }

    public TaskDAO getTask(){
        return null;
    }
}
