package org.nchc.bigdata.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/3/2.
 */
public class MRLogFileFilter extends LogFileFilter {

    public MRLogFileFilter(Configuration conf) throws IOException, SQLException {
        super(conf, "MR");
    }

    @Override
    public boolean accept(Path path) {
        if(path.getName().endsWith(".xml"))
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
