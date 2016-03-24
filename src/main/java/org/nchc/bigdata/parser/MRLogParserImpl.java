package org.nchc.bigdata.parser;

import com.google.gson.*;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.model.MRJobModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by 1403035 on 2016/3/2.
 */
public class MRLogParserImpl implements IParser {
    private static Logger logger = Logger.getLogger(MRLogParserImpl.class);

    private Gson gson;
    private JsonParser parser;
    private List<String> eventNames;
    private MRJobModel eventLogs;

    public MRLogParserImpl(){
        this.gson = new Gson();
        this.parser = new JsonParser();
        this.eventLogs = new MRJobModel();
        this.eventNames = new ArrayList<>();

        // default event logs
        this.eventNames.add(Const.AM_STARTED);

        this.eventNames.add(Const.JOB_SUBMITTED);
        this.eventNames.add(Const.JOB_FINISHED);
        this.eventNames.add(Const.JOB_FAILED);
        this.eventNames.add(Const.JOB_KILLED);
        this.eventNames.add(Const.JOB_ERROR);

        this.eventNames.add(Const.MAP_ATTEMPT_STARTED);
        this.eventNames.add(Const.MAP_ATTEMPT_FINISHED);
        this.eventNames.add(Const.MAP_ATTEMPT_FAILED);
        this.eventNames.add(Const.MAP_ATTEMPT_KILLED);

        this.eventNames.add(Const.REDUCE_ATTEMPT_STARTED);
        this.eventNames.add(Const.REDUCE_ATTEMPT_FINISHED);
        this.eventNames.add(Const.REDUCE_ATTEMPT_FAILED);
        this.eventNames.add(Const.REDUCE_ATTEMPT_KILLED);

    }

    @Override
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

        if(element.isJsonObject()) {
            JsonObject asJsonObject = element.getAsJsonObject();
            String type = asJsonObject.get("type").getAsString();

            if((i = this.eventNames.indexOf(type)) != -1){
                String eventName = this.eventNames.get(i);
                JsonObject obj2 = asJsonObject.get("event").getAsJsonObject();
                Iterator<Map.Entry<String, JsonElement>> it = obj2.entrySet().iterator();
                Map.Entry<String,JsonElement> entry = it.next();

                String eventData = entry.getValue().toString();

                switch(eventName){
                    case Const.AM_STARTED:
                        eventLogs.setAmStarted(this.gson.fromJson(eventData, MRJobModel.AMStarted.class));
                        break;
                    case Const.JOB_SUBMITTED:
                        eventLogs.setJobSubmitted(this.gson.fromJson(eventData, MRJobModel.JobSubmitted.class));
                        break;
                    case Const.JOB_FINISHED:
                        eventLogs.setJobFinished(this.gson.fromJson(eventData, MRJobModel.JobFinished.class));
                        break;
                    case Const.JOB_FAILED:
                    case Const.JOB_KILLED:
                    case Const.JOB_ERROR:
                        eventLogs.setJobUnsuccessfulCompletion(this.gson.fromJson(eventData, MRJobModel.JobUnsuccessfulCompletion.class));
                        break;
                    // Map attempt Start/Complete event
                    case Const.MAP_ATTEMPT_STARTED:
                        eventLogs.addMapAttemptStarted(this.gson.fromJson(eventData, MRJobModel.TaskAttemptStarted.class));
                        break;
                    case Const.MAP_ATTEMPT_FAILED:
                    case Const.MAP_ATTEMPT_KILLED:
                        eventLogs.addMapAttemptCompleted(this.gson.fromJson(eventData, MRJobModel.TaskAttemptUnsuccessfulCompletion.class));
                        break;
                    case Const.MAP_ATTEMPT_FINISHED:
                        eventLogs.addMapAttemptCompleted(this.gson.fromJson(eventData, MRJobModel.MapAttemptFinished.class));
                        break;
                    // Reduce attempt Start/Complete event
                    case Const.REDUCE_ATTEMPT_STARTED:
                        eventLogs.addReduceAttemptStarted(this.gson.fromJson(eventData, MRJobModel.TaskAttemptStarted.class));
                        break;
                    case Const.REDUCE_ATTEMPT_KILLED:
                    case Const.REDUCE_ATTEMPT_FAILED:
                        eventLogs.addReduceAttemptCompleted(this.gson.fromJson(eventData, MRJobModel.TaskAttemptUnsuccessfulCompletion.class));
                        break;
                    case Const.REDUCE_ATTEMPT_FINISHED:
                        eventLogs.addReduceAttemptCompleted(this.gson.fromJson(eventData, MRJobModel.ReduceAttemptFinished.class));
                        break;
                }
            }
        }else {
            logger.debug("{" + line + "} is not valid json format.");
        }
        return true;
    }

    @Override
    public void clear() {
        eventLogs.clean();
    }

    @Override
    public MRJobModel result() {
        return (MRJobModel) eventLogs.deepClone();
    }
}
