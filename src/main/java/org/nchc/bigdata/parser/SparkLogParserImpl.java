package org.nchc.bigdata.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.db.JobDAO;
import org.nchc.bigdata.db.SparkJobDAO;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/2.
 */
public class SparkLogParserImpl implements IParser {

    private static Logger logger = Logger.getLogger(SparkLogParserImpl.class);

    public final static String APP_START       = "SparkListenerApplicationStart";
    public final static String JOB_START       = "SparkListenerJobStart";
    public final static String STAGE_SUBMIT    = "SparkListenerStageSubmitted";
    public final static String EXECUTOR_ADD    = "SparkListenerExecutorAdded";
    public final static String TASK_START      = "SparkListenerTaskStart";
    public final static String TASK_END        = "SparkListenerTaskEnd";
    public final static String STAGE_COMPLETED = "SparkListenerStageCompleted";
    public final static String JOB_END         = "SparkListenerJobEnd";
    public final static String APP_END         = "SparkListenerApplicationEnd";
    public final static String BLOCKMANAGER_ADD = "SparkListenerBlockManagerAdded";

    /***
     * if this parser find the event and must return true to perform the "event found!, so you can skip post parser."
     */
    private Gson gson;
    private JsonParser parser;
    private List<String> eventNames;
    private SparkJobDAO eventLogs;




    /**
     * default SparkEventLogParser collect executor and task information
     * if you want to collect another event, please create another constructor for your case.
     **/
    public SparkLogParserImpl(Configuration conf){
        this.gson = new Gson();
        this.parser = new JsonParser();
        this.eventLogs = new SparkJobDAO(conf);
        //default event log, APP_START & APP_END & TASK_START & TASK_END
        this.eventNames = new ArrayList<String>();
        this.eventNames.add(APP_START);
        this.eventNames.add(APP_END);
        this.eventNames.add(TASK_START);
        this.eventNames.add(TASK_END);
    }

    public SparkLogParserImpl(Configuration conf, List<String> eventNames){
        this.gson = new Gson();
        this.parser = new JsonParser();
        this.eventLogs = new SparkJobDAO(conf);
        this.eventNames = new ArrayList<String>(eventNames.size());
        for(String eventName: eventNames){
            this.eventNames.add(eventName);
        }
    }

    public boolean parse(String line){
        int i = -1;
        JsonElement element = null;
        try{
            element = parser.parse(line);
        } catch(Exception e){
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
            return false;
        }

        if(element.isJsonObject()){
            JsonObject asJsonObject = element.getAsJsonObject();
            String event = asJsonObject.get("Event").getAsString();

            if((i = this.eventNames.indexOf(event)) != -1){
                String eventName = this.eventNames.get(i);
                switch(eventName){
                    case JOB_START:
                        this.eventLogs.addJobStart(this.gson.fromJson(element, SparkJobDAO.JobStart.class));
                        break;
                    case EXECUTOR_ADD:
                        this.eventLogs.addExecutorAdd(this.gson.fromJson(element, SparkJobDAO.ExecutorAdded.class));
                        break;
                    case TASK_START:
                        this.eventLogs.addTaskStart(this.gson.fromJson(element, SparkJobDAO.TaskStart.class));
                        break;
                    case TASK_END:
                        this.eventLogs.addTaskEnd(this.gson.fromJson(element, SparkJobDAO.TaskEnd.class));
                        break;
                    case APP_START:
                        this.eventLogs.setAppStart(this.gson.fromJson(element, SparkJobDAO.AppStart.class));
                        break;
                    case APP_END:
                        this.eventLogs.setAppEnd(this.gson.fromJson(element, SparkJobDAO.AppEnd.class));
                        break;
                    case JOB_END:
                        this.eventLogs.addJobEnd(this.gson.fromJson(element, SparkJobDAO.JobEnd.class));
                        break;
                    case STAGE_SUBMIT:
                        this.eventLogs.addStageSubmit(this.gson.fromJson(element, SparkJobDAO.StageSubmit.class));
                        break;
                    case BLOCKMANAGER_ADD:
                        this.eventLogs.addBlocKManager(this.gson.fromJson(element, SparkJobDAO.BlockManager.class));
                        break;
                    case STAGE_COMPLETED:
                        this.eventLogs.addStageComplete(this.gson.fromJson(element, SparkJobDAO.StageCompleted.class));
                }
            }
        } else {
            logger.error(line + " is not a json foramt");
        }
        return true;
    }

    public SparkJobDAO result(){
        return eventLogs;
    }
}
