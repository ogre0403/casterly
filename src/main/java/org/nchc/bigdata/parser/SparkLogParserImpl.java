package org.nchc.bigdata.parser;

import com.google.gson.*;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.model.SparkJobModel;

import java.io.IOException;
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
    public final static String EXECUTOR_REMOVED    = "SparkListenerExecutorRemoved";
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
    private SparkJobModel eventLogs;




    /**
     * default SparkEventLogParser collect executor and task information
     * if you want to collect another event, please create another constructor for your case.
     **/
    public SparkLogParserImpl(){
        this.gson = new Gson();
        this.parser = new JsonParser();
        this.eventLogs = new SparkJobModel();
        //default event log, APP_START & APP_END & TASK_START & TASK_END
        this.eventNames = new ArrayList<>();
        this.eventNames.add(APP_START);
        this.eventNames.add(APP_END);
        this.eventNames.add(EXECUTOR_ADD);
        this.eventNames.add(EXECUTOR_REMOVED);
        this.eventNames.add(TASK_START);
        this.eventNames.add(TASK_END);
    }

    public SparkLogParserImpl(List<String> eventNames){
        this.gson = new Gson();
        this.parser = new JsonParser();
        this.eventLogs = new SparkJobModel();
        this.eventNames = new ArrayList<>(eventNames.size());
        for(String eventName: eventNames){
            this.eventNames.add(eventName);
        }
    }

    public boolean parse(String line) {
        int i = -1;
        JsonElement element = null;
        try{
            element = parser.parse(line);
        } catch(JsonSyntaxException e){
            logger.error("Can not parse following content:");
            logger.error(line);
            logger.error(Util.traceString(e));
            return false;
        }

        if(element.isJsonObject()){
            JsonObject asJsonObject = element.getAsJsonObject();
            String event = asJsonObject.get("Event").getAsString();

            if((i = this.eventNames.indexOf(event)) != -1){
                String eventName = this.eventNames.get(i);
                switch(eventName){
                    case JOB_START:
                        this.eventLogs.addJobStart(this.gson.fromJson(element, SparkJobModel.JobStart.class));
                        break;
                    case EXECUTOR_ADD:
                        this.eventLogs.addExecutorAdd(this.gson.fromJson(element, SparkJobModel.ExecutorAdded.class));
                        break;
                    case EXECUTOR_REMOVED:
                        this.eventLogs.addExecutorRemoved(this.gson.fromJson(element, SparkJobModel.ExecutorRemoved.class));
                        break;
                    case TASK_START:
                        this.eventLogs.addTaskStart(this.gson.fromJson(element, SparkJobModel.TaskStart.class));
                        break;
                    case TASK_END:
                        this.eventLogs.addTaskEnd(this.gson.fromJson(element, SparkJobModel.TaskEnd.class));
                        break;
                    case APP_START:
                        this.eventLogs.setAppStart(this.gson.fromJson(element, SparkJobModel.AppStart.class));
                        break;
                    case APP_END:
                        this.eventLogs.setAppEnd(this.gson.fromJson(element, SparkJobModel.AppEnd.class));
                        break;
                    case JOB_END:
                        this.eventLogs.addJobEnd(this.gson.fromJson(element, SparkJobModel.JobEnd.class));
                        break;
                    case STAGE_SUBMIT:
                        this.eventLogs.addStageSubmit(this.gson.fromJson(element, SparkJobModel.StageSubmit.class));
                        break;
                    case BLOCKMANAGER_ADD:
                        this.eventLogs.addBlocKManager(this.gson.fromJson(element, SparkJobModel.BlockManager.class));
                        break;
                    case STAGE_COMPLETED:
                        this.eventLogs.addStageComplete(this.gson.fromJson(element, SparkJobModel.StageCompleted.class));
                }
            }
        } else {
            logger.error(line + " is not a json foramt");
        }
        return true;
    }

    @Override
    public void clear() {
        eventLogs.clean();
    }

    public SparkJobModel result(){
        return (SparkJobModel) eventLogs.deepClone();
    }
}
