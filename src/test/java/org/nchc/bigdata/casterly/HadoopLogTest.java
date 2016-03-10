package org.nchc.bigdata.casterly;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.data.Json;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.mapreduce.jobhistory.AMStarted;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinished;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.nchc.bigdata.model.MRJobModel;
import org.nchc.bigdata.parser.MRLogParserImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class HadoopLogTest {
    protected static Logger logger = Logger.getLogger(HadoopLogTest.class);

    public static String filename = "job_1456378629209_0161-1456902161824-u11lss02-streamjob3184777771968013509.jar-1456902176126-2-0-SUCCEEDED-root.u11lss02-1456902167822.jhist";

    @Test
    public void testParser() throws IOException {
        MRLogParserImpl mrLogParser = new MRLogParserImpl();
        InputStream is = SparkLogTest.class.getClass().getResourceAsStream("/" + filename);


        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        while(null != (line=br.readLine())){
            mrLogParser.parse(line);
        }


        MRJobModel mr = mrLogParser.result();
        Assert.assertEquals(mr.getMapAttemptFinishMap().size(),
                mr.getMapAttemptStartMap().size());
        Assert.assertEquals(mr.getReduceAttemptFinishMap().size(),
                mr.getReduceAttemptStartMap().size());

        for (Map.Entry<String, MRJobModel.TaskAttemptCompleted> entry
                : mr.getMapAttemptFinishMap().entrySet()) {
            logger.info(entry.getValue().getAttemptId());
            logger.info(entry.getValue().getFinishTime());
        }
    }






}
