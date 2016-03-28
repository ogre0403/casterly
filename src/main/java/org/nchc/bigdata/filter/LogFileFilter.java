package org.nchc.bigdata.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.dao.ConnectionFactory;
import org.nchc.bigdata.dao.DBUtil;

import java.io.IOException;
import java.sql.*;

/**
 * Created by 1403035 on 2016/2/2.
 */
public abstract class LogFileFilter implements PathFilter {

    protected static Logger logger = Logger.getLogger(LogFileFilter.class);
    protected FileSystem fs;
    protected Connection connection;
    protected Configuration config;
    protected String type;

    /**
     * The maximum modification time of a file to be accepted in milliseconds
     * since January 1, 1970 UTC (including).
     */
    private long lastProcessedFileModifiedTime = 0;
    private long largestTimeStamp = 0;

    public LogFileFilter(Configuration conf, String type) throws IOException, SQLException {
        this.config = conf;
        this.fs = FileSystem.get(conf);
        this.connection = ConnectionFactory.getConnection(this.config);
        this.type = type;
        long lastTime = getLastProcessedFileModifiedTimeFromDB();
        this.lastProcessedFileModifiedTime = lastTime;
        this.largestTimeStamp = lastTime;
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

    public long getLastProcessedFileModifiedTime(){
        return  lastProcessedFileModifiedTime;
    }

    public long getLastProcessedFileModifiedTimeFromDB() throws SQLException {
        PreparedStatement statement = null;
        ResultSet rs = null ;
        long result = 0L;
        try {
            statement = connection.prepareStatement(Const.SQL_TEMPLATE_GET_LASTPROCESSED);
            statement.setString(1, type);
            rs = statement.executeQuery();
            if(rs.next()) {
                result = rs.getLong("LAST");
            }
        }finally {
            DBUtil.close(statement);
            DBUtil.close(rs);
        }
        return result;
    }

    public boolean saveLastProcessedTime()  {
        this.lastProcessedFileModifiedTime = this.largestTimeStamp;
        PreparedStatement prestate= null;
        try {
            prestate = connection.prepareStatement(Const.SQL_TEMPLATE_UPDATE_LASTPROCESSED);
            prestate.setLong(1, this.lastProcessedFileModifiedTime);
            prestate.setString(2, this.type);
            prestate.executeUpdate();
        }catch (SQLException sqle){
            logger.warn("save to DB fail");
            logger.warn(Util.traceString(sqle));
            return false;
        }finally {
            DBUtil.close(prestate);
        }
        return true;
    }
}
