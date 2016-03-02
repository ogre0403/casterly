package org.nchc.bigdata.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.dao.ConnectionFactory;
import org.nchc.bigdata.dao.DBUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by 1403035 on 2016/2/2.
 */
public abstract class LogFileFilter implements PathFilter {

    protected static Logger logger = Logger.getLogger(LogFileFilter.class);
    protected FileSystem fs;
    protected Connection connection;
    protected Statement  statement;
    protected Configuration config;

    /**
     * The maximum modification time of a file to be accepted in milliseconds
     * since January 1, 1970 UTC (including).
     */
    private long lastProcessedFileModifiedTime = 0;
    private long largestTimeStamp = 0;

    public LogFileFilter(Configuration conf) throws IOException {
        this.config = conf;
        fs = FileSystem.get(conf);
    }

    public void setFileSystem(FileSystem fs){
        this.fs = fs ;
    }

    public boolean accept(long fileModificationTimeMillis) {
        if (lastProcessedFileModifiedTime < fileModificationTimeMillis) {
            if (largestTimeStamp < fileModificationTimeMillis)
                largestTimeStamp = fileModificationTimeMillis;
            return true;
        }else
            return false;
    }

    public void updateLastTimeStamp(){
        lastProcessedFileModifiedTime = largestTimeStamp;
    }

    public long getLastProcessedFileModifiedTime(){
        return  lastProcessedFileModifiedTime;
    }

    public void setLastProcessedFileModifiedTime(long lastTime){
        this.lastProcessedFileModifiedTime = lastTime;
    }

    public long getLastProcessedFileModifiedTimeFromDB() throws SQLException {
        ResultSet rs = null ;
        long result = 0L;
        try {
            connection = ConnectionFactory.getConnection(this.config);
            statement = connection.createStatement();
            String query = Const.SQL_TEMPLATE_GET_LASTPROCESSED;
            rs = statement.executeQuery(query);

            if(rs.next()) {
                result = rs.getLong("LAST");
            }
        }finally {
            DBUtil.close(connection);
            DBUtil.close(statement);
            DBUtil.close(rs);
        }
            return result;
    }
}
