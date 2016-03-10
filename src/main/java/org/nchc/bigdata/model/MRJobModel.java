package org.nchc.bigdata.model;


import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 1403035 on 2016/3/2.
 */
public class MRJobModel extends JobModel {

    private AMStarted amStarted;
    private JobSubmitted jobSubmitted;
    private JobFinished jobFinished;
    private JobUnsuccessfulCompletion jobUnsuccessfulCompletion;

    // map key = 000000_2  , reduce key = 000000_2
    private Map<String, TaskAttemptStarted> mapAttemptStartMap;
    private Map<String, TaskAttemptCompleted> mapAttemptFinishMap;
    private Map<String, TaskAttemptStarted> reduceAttemptStartMap;
    private Map<String, TaskAttemptCompleted> reduceAttemptFinishMap;

    public MRJobModel(){
        mapAttemptStartMap = new HashMap<>();
        mapAttemptFinishMap = new HashMap<>();
        reduceAttemptStartMap = new HashMap<>();
        reduceAttemptFinishMap = new HashMap<>();
    }

    public AMStarted getAmStarted() {
        return amStarted;
    }

    public void setAmStarted(AMStarted amStarted) {
        this.amStarted = amStarted;
    }

    public JobSubmitted getJobSubmitted() {
        return jobSubmitted;
    }

    public void setJobSubmitted(JobSubmitted jobSubmitted) {
        this.jobSubmitted = jobSubmitted;
    }

    public JobFinished getJobFinished() {
        return jobFinished;
    }

    public void setJobFinished(JobFinished jobFinished) {
        this.jobFinished = jobFinished;
    }

    public JobUnsuccessfulCompletion getJobUnsuccessfulCompletion() {
        return jobUnsuccessfulCompletion;
    }

    public void setJobUnsuccessfulCompletion(JobUnsuccessfulCompletion jobUnsuccessfulCompletion) {
        this.jobUnsuccessfulCompletion = jobUnsuccessfulCompletion;
    }

    public void addMapAttemptStarted(TaskAttemptStarted attemptStart){
        this.addAttemptStart(this.mapAttemptStartMap, attemptStart);
    }

    public void addReduceAttemptStarted(TaskAttemptStarted attemptStarted){
        this.addAttemptStart(this.reduceAttemptStartMap, attemptStarted);
    }

    private String attemptIdPostfix(String attemptID){
        String[] rr = attemptID.split("_");
        return rr[rr.length-2] +"_" +rr[rr.length-1];
    }

    private void addAttemptStart(Map<String, TaskAttemptStarted> map,
                                 TaskAttemptStarted attemptStarted){
        String postfix = attemptIdPostfix(attemptStarted.getAttemptId());
        map.put(postfix, attemptStarted);
    }

    public void addMapAttemptCompleted(TaskAttemptCompleted completed){
        this.addAttemptCompleted(this.mapAttemptFinishMap, completed);
    }

    public void addReduceAttemptCompleted(TaskAttemptCompleted completed){
        this.addAttemptCompleted(this.reduceAttemptFinishMap, completed);
    }

    private void addAttemptCompleted(Map<String, TaskAttemptCompleted>map,
                                     TaskAttemptCompleted completed){
        String postfix = attemptIdPostfix(completed.getAttemptId());
        map.put(postfix, completed);
    }

    public Map<String, TaskAttemptStarted> getMapAttemptStartMap(){
        return this.mapAttemptStartMap;
    }

    public Map<String, TaskAttemptCompleted> getMapAttemptFinishMap(){
        return mapAttemptFinishMap;
    }

    public Map<String, TaskAttemptStarted> getReduceAttemptStartMap(){
        return this.reduceAttemptStartMap;
    }

    public Map<String, TaskAttemptCompleted> getReduceAttemptFinishMap(){
        return this.reduceAttemptFinishMap;
    }

    @Override
    public void clean() {
        mapAttemptStartMap.clear();
        mapAttemptFinishMap.clear();
        reduceAttemptStartMap.clear();
        reduceAttemptFinishMap.clear();
        amStarted = null;
        jobSubmitted = null;
        jobFinished = null;
        jobUnsuccessfulCompletion = null;
    }

    public class AMStarted implements Serializable {
        @SerializedName("applicationAttemptId")private String applicationAttemptId;
        @SerializedName("startTime")private long startTime;
        @SerializedName("containerId")private String containerId;
        @SerializedName("nodeManagerHost")private String nodeManagerHost;
        @SerializedName("nodeManagerPort")private int nodeManagerPort;
        @SerializedName("nodeManagerHttpPort")private int nodeManagerHttpPort;

        public String getApplicationAttemptId() {
            return applicationAttemptId;
        }

        public void setApplicationAttemptId(String applicationAttemptId) {
            this.applicationAttemptId = applicationAttemptId;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public String getContainerId() {
            return containerId;
        }

        public void setContainerId(String containerId) {
            this.containerId = containerId;
        }

        public String getNodeManagerHost() {
            return nodeManagerHost;
        }

        public void setNodeManagerHost(String nodeManagerHost) {
            this.nodeManagerHost = nodeManagerHost;
        }

        public int getNodeManagerPort() {
            return nodeManagerPort;
        }

        public void setNodeManagerPort(int nodeManagerPort) {
            this.nodeManagerPort = nodeManagerPort;
        }

        public int getNodeManagerHttpPort() {
            return nodeManagerHttpPort;
        }

        public void setNodeManagerHttpPort(int nodeManagerHttpPort) {
            this.nodeManagerHttpPort = nodeManagerHttpPort;
        }
    }

    public class JobSubmitted implements Serializable{
        @SerializedName("jobid")private String jobid;
        @SerializedName("jobName")private String jobName;
        @SerializedName("userName")private String userName;
        @SerializedName("submitTime")private long submitTime;
        @SerializedName("jobQueueName")private String jobQueueName;

        public String getJobid() {
            return jobid;
        }

        public void setJobid(String jobid) {
            this.jobid = jobid;
        }

        public String getJobName() {
            return jobName;
        }

        public void setJobName(String jobName) {
            this.jobName = jobName;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public long getSubmitTime() {
            return submitTime;
        }

        public void setSubmitTime(long submitTime) {
            this.submitTime = submitTime;
        }

        public String getJobQueueName() {
            return jobQueueName;
        }

        public void setJobQueueName(String jobQueueName) {
            this.jobQueueName = jobQueueName;
        }
    }

    public class JobFinished implements Serializable{
        @SerializedName("jobid")private String jobid;
        @SerializedName("finishTime")private long finishTime;
        @SerializedName("finishedMaps")private long finishedMaps;
        @SerializedName("finishedReduces")private long finishedReduces;
        @SerializedName("failedMaps")private long failedMaps;
        @SerializedName("failedReduces")private long failedReduces;

        public String getJobid() {
            return jobid;
        }

        public void setJobid(String jobid) {
            this.jobid = jobid;
        }

        public long getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(long finishTime) {
            this.finishTime = finishTime;
        }

        public long getFinishedMaps() {
            return finishedMaps;
        }

        public void setFinishedMaps(long finishedMaps) {
            this.finishedMaps = finishedMaps;
        }

        public long getFinishedReduces() {
            return finishedReduces;
        }

        public void setFinishedReduces(long finishedReduces) {
            this.finishedReduces = finishedReduces;
        }

        public long getFailedMaps() {
            return failedMaps;
        }

        public void setFailedMaps(long failedMaps) {
            this.failedMaps = failedMaps;
        }

        public long getFailedReduces() {
            return failedReduces;
        }

        public void setFailedReduces(long failedReduces) {
            this.failedReduces = failedReduces;
        }
    }

    public class JobUnsuccessfulCompletion implements Serializable{
        @SerializedName("jobid")private String jobid;
        @SerializedName("finishTime")private String finishTime;
        @SerializedName("finishedMaps")private String finishedMaps;
        @SerializedName("finishedReduces")private String finishedReduces;
        @SerializedName("jobStatus")private String jobStatus;
        @SerializedName("diagnostics")private String diagnostics;

        public String getJobid() {
            return jobid;
        }

        public void setJobid(String jobid) {
            this.jobid = jobid;
        }

        public String getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(String finishTime) {
            this.finishTime = finishTime;
        }

        public String getFinishedMaps() {
            return finishedMaps;
        }

        public void setFinishedMaps(String finishedMaps) {
            this.finishedMaps = finishedMaps;
        }

        public String getFinishedReduces() {
            return finishedReduces;
        }

        public void setFinishedReduces(String finishedReduces) {
            this.finishedReduces = finishedReduces;
        }

        public String getJobStatus() {
            return jobStatus;
        }

        public void setJobStatus(String jobStatus) {
            this.jobStatus = jobStatus;
        }

        public String getDiagnostics() {
            return diagnostics;
        }

        public void setDiagnostics(String diagnostics) {
            this.diagnostics = diagnostics;
        }
    }

    public abstract class TaskAttempt implements Serializable{
        @SerializedName("taskid")private String taskid;
        @SerializedName("taskType")private String taskType;
        @SerializedName("attemptId")private String attemptId;
        public String getTaskid() {
            return taskid;
        }

        public void setTaskid(String taskid) {
            this.taskid = taskid;
        }

        public String getTaskType() {
            return taskType;
        }

        public void setTaskType(String taskType) {
            this.taskType = taskType;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }
    }

    public class TaskAttemptStarted extends TaskAttempt{
        @SerializedName("startTime")private long startTime;
        @SerializedName("trackerName")private String trackerName;
        @SerializedName("httpPort")private int httpPort;
        @SerializedName("shufflePort")private int shufflePort;
        @SerializedName("containerId")private String containerId;

    }

    public abstract class TaskAttemptCompleted extends TaskAttempt{
        @SerializedName("finishTime")private long finishTime;
        @SerializedName("hostname")private String hostname;
        @SerializedName("port")private int port;
        @SerializedName("rackname")private String rackname;

        public long getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(long finishTime) {
            this.finishTime = finishTime;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getRackname() {
            return rackname;
        }

        public void setRackname(String rackname) {
            this.rackname = rackname;
        }
    }

    public class ReduceAttemptFinished extends TaskAttemptCompleted {
        @SerializedName("taskStatus")private String taskStatus;
        @SerializedName("shuffleFinishTime")private long shuffleFinishTime;
        @SerializedName("sortFinishTime")private long sortFinishTime;
        @SerializedName("state")private String state;

        public String getTaskStatus() {
            return taskStatus;
        }

        public void setTaskStatus(String taskStatus) {
            this.taskStatus = taskStatus;
        }

        public long getShuffleFinishTime() {
            return shuffleFinishTime;
        }

        public void setShuffleFinishTime(long shuffleFinishTime) {
            this.shuffleFinishTime = shuffleFinishTime;
        }

        public long getSortFinishTime() {
            return sortFinishTime;
        }

        public void setSortFinishTime(long sortFinishTime) {
            this.sortFinishTime = sortFinishTime;
        }


        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    public class MapAttemptFinished extends TaskAttemptCompleted {
        @SerializedName("taskStatus")private String taskStatus;
        @SerializedName("mapFinishTime")private long mapFinishTime;
        @SerializedName("state")private String state;

        public String getTaskStatus() {
            return taskStatus;
        }

        public void setTaskStatus(String taskStatus) {
            this.taskStatus = taskStatus;
        }

        public long getMapFinishTime() {
            return mapFinishTime;
        }

        public void setMapFinishTime(long mapFinishTime) {
            this.mapFinishTime = mapFinishTime;
        }


        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    public class TaskAttemptUnsuccessfulCompletion extends TaskAttemptCompleted implements Serializable{
        @SerializedName("status")private String status;
        @SerializedName("error")private String error;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }
    }
}
