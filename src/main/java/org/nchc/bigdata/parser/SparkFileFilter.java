package org.nchc.bigdata.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class SparkFileFilter implements PathFilter {

    private static Logger logger = Logger.getLogger(SparkFileFilter.class);
    private Configuration config;

    /**
     * The maximum modification time of a file to be accepted in milliseconds
     * since January 1, 1970 UTC (including).
     */
    private long maxModificationTimeMillis = 0;


    private int logType = 1;

    public SparkFileFilter(Configuration config) throws SQLException {
        this.config = config;
    }


    public boolean accept(Path path) {
        if(path.getName().endsWith(".inprogress"))
            return false;

        try {
            FileSystem fs = path.getFileSystem(this.config);
            FileStatus fileStatus = fs.getFileStatus(path);
            long fileModificationTimeMillis = fileStatus.getModificationTime();
            return accept(fileModificationTimeMillis);
        } catch (IOException e) {
            logger.error("Can not check existence");
            return false;
        }
    }

    public boolean accept(long fileModificationTimeMillis) {
        return (maxModificationTimeMillis < fileModificationTimeMillis);
    }

}
