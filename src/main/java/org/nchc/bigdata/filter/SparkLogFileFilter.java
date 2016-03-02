package org.nchc.bigdata.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/3/1.
 */
public class SparkLogFileFilter extends LogFileFilter {

    public SparkLogFileFilter(Configuration conf) throws IOException, SQLException {
        super(conf);
        long lastTime = getLastProcessedFileModifiedTimeFromDB();
        setLastProcessedFileModifiedTime(lastTime);

    }

    public boolean accept(Path path) {
        if(path.getName().endsWith(".inprogress"))
            return false;

        try {
            FileStatus fileStatus = fs.getFileStatus(path);
            long fileModificationTimeMillis = fileStatus.getModificationTime();
            return accept(fileModificationTimeMillis);
        } catch (IOException e) {
            logger.error("Can not check existence");
            return false;
        }
    }
}
