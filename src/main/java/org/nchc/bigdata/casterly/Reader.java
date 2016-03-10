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

    private Connection connection;
    private Statement statement;


    public Reader(){}

    public Reader(Configuration config) {
        this.config = config;
        try {
            this.fs = FileSystem.get(config);
        }catch (IOException e){

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

    public List<JobModel> readAllFile() throws IOException{
        FileStatus[] allFileState = fs.listStatus(this.logDir, filter);
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

        filter.updateLastTimeStamp();
        if( saveLastProcessedTime(filter.getLastProcessedFileModifiedTime()) == true) {
            // LastProcessedTime save to DB ok, return models
            return models;
        }else {
            // LastProcessedTime DON'T save to DB ok, return empty model list
            return new ArrayList<JobModel>();
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

    public boolean saveLastProcessedTime(long lastProcessedTime) {
        ResultSet rs = null ;
        try {
            connection = ConnectionFactory.getConnection(config);
            statement = connection.createStatement();
            String query = String.format(
                    Const.SQL_TEMPLATE_UPDATE_LASTPROCESSED, lastProcessedTime);
            rs = statement.executeQuery(query);

        }catch (SQLException sqle){
            logger.warn("save to DB fail");
            logger.warn(Util.traceString(sqle));
            return false;
        }finally {
            DBUtil.close(connection);
            DBUtil.close(statement);
            DBUtil.close(rs);
        }
        return true;
    }

}
