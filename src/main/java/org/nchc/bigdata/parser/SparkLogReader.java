package org.nchc.bigdata.parser;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.nchc.bigdata.db.SparkJobDAO;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class SparkLogReader extends Reader {

    private static Logger logger = Logger.getLogger(SparkLogReader.class);


    public SparkLogReader(Configuration config, Path path) throws IOException, SQLException {
        super(config,path);

        parser = new SparkLogParserImpl(config);
        filter = new SparkFileFilter(config);

    }


    public SparkLogReader(){

    }

    public SparkJobDAO read(BufferedReader br) throws IOException {
        String line;
        while(null != (line=br.readLine())){
            parser.parse(line);
        }
        return parser.result();
    }
}
