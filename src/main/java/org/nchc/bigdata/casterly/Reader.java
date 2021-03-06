package org.nchc.bigdata.casterly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.nchc.bigdata.dao.ConnectionFactory;
import org.nchc.bigdata.dao.DBUtil;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.parser.IParser;
import org.nchc.bigdata.filter.LogFileFilter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class Reader {

    protected static Logger logger = Logger.getLogger(Reader.class);
    protected IParser parser = null;
    protected LogFileFilter filter = null;
    protected Configuration config;
    protected FileSystem fs;
    protected Path logDir;

    public Reader(){}

    public Reader(Configuration config) {
        this.config = config;
        try {
            this.fs = FileSystem.get(config);
        }catch (IOException e){
            logger.error(Util.traceString(e));
        }
    }

    public Reader(Configuration config, Path path) {
        this(config);
        setPath(path);
    }

    public void setFilter(LogFileFilter filter){
        this.filter = filter;
    }

    public void setParser(IParser parser){
        this.parser = parser;
    }

    public void setPath(Path path){
        this.logDir = path;
    }

    public List<JobModel> readAllFile(boolean isRecursive) throws IOException{
        FileStatus[] allFileState = listFiles(isRecursive);
        List<JobModel> models = new ArrayList<>();
        FSDataInputStream fis = null;
        BufferedReader br = null;
        for(FileStatus status: allFileState) {
            try {
                fis = fs.open(status.getPath());
                br = new BufferedReader(new InputStreamReader(fis));
                models.add(read(br));
            }finally {
                if(fis != null)
                    fis.close();
                if(br != null)
                    br.close();
            }
        }

        if( filter.saveLastProcessedTime() == true) {
            // LastProcessedTime save to DB ok, return models
            return models;
        }else {
            // LastProcessedTime DON'T save to DB ok, return empty model list
            return new ArrayList<JobModel>();
        }
    }

    private  FileStatus[] listFiles(boolean isRecursive) throws IOException {
        if(isRecursive){
            List<FileStatus> fileStatusesList = new ArrayList<FileStatus>();
            traverseDirs(fileStatusesList, this.fs, this.logDir, this.filter);
            FileStatus[] fileStatuses = (FileStatus[]) fileStatusesList.toArray(
                    new FileStatus[fileStatusesList.size()]);
            return fileStatuses;
        }else{
            return fs.listStatus(this.logDir, filter);
        }
    }

    private void traverseDirs(List<FileStatus> fileStatusesList, FileSystem hdfs,
                              Path inputPath, LogFileFilter filter) throws IOException {
        // get all the files and dirs in the current dir
        FileStatus allFiles[] = hdfs.listStatus(inputPath);
        for (FileStatus aFile: allFiles) {
            if (aFile.isDir()) {
                //recurse here
                traverseDirs(fileStatusesList, hdfs, aFile.getPath(), filter);
            }
            else {
                // check if the pathFilter is accepted for this file
                if (filter.accept(aFile.getPath())) {
                    fileStatusesList.add(aFile);
                }
            }
        }
    }

    public < T extends JobModel> T read(BufferedReader br) throws IOException {
        String line;
        parser.clear();
        while(null != (line=br.readLine())){
            parser.parse(line);
        }
        return parser.result();
    }

    public void close(){

    }
}
