package org.nchc.bigdata.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.nchc.bigdata.model.JobModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class Reader {

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

    public void setFilter(PathFilter filter){
        this.filter = filter;
    }

    public void setParser(IParser parser){
        this.parser = parser;
    }

    public void setPath(Path path){
        this.logDir = path;
    }

    public List<JobModel> readAllFile() throws IOException, SQLException {
        FileStatus[] allFileState = fs.listStatus(this.logDir, filter);
        List<JobModel> models = new ArrayList<JobModel>();
        for(FileStatus status: allFileState) {
            FSDataInputStream fis = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            models.add(read(br));
        }
        return models;
    }

    public < T extends JobModel> T read(BufferedReader br) throws IOException {
        String line;
        while(null != (line=br.readLine())){
            parser.parse(line);
        }
        return parser.result();
    }

}
