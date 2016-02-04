package org.nchc.bigdata.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.nchc.bigdata.db.JobDAO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by 1403035 on 2016/2/2.
 */
public abstract class Reader {

    protected IParser parser = null;
    protected PathFilter filter = null;
    protected Configuration config;
    protected FileSystem fs;
    protected Path logDir;

    protected long minProcessedFileTime = 0;
    protected long maxProcessedFileTIme = Long.MAX_VALUE;

    public Reader(){}

    public Reader(Configuration config) throws IOException {
        this.config = config;
        //this.fs = FileSystem.get(config);
    }

    public Reader(Configuration config, Path path) throws IOException {
        this(config);
        setPath(path);
    }

    public void readAllFile() throws IOException {
        FileStatus[] allFileState = fs.listStatus(this.logDir, filter);
        for(FileStatus status: allFileState) {
            FSDataInputStream fis = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            JobDAO job = read(br);
            persist(job);
        }
    }

    public void setPath(Path path){
        this.logDir = path;
    }

    protected abstract <T extends JobDAO> T read(BufferedReader br) throws IOException;

    public void persist(JobDAO  job){
        job.writeJob();
    }

}
