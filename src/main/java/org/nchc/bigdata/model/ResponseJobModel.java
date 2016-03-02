package org.nchc.bigdata.model;

/**
 * Created by 1403035 on 2016/2/19.
 */
public class ResponseJobModel extends JobModel{

    private String user;
    private String queue;
    private String jobName;
    private long submit_time;
    private long finish_time;
    private long cpuHour;

    // for MR
    private long map_num;
    private long reduce_num;

    // for spark
    private long executor_num;
    private long task_num;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getSubmit_time() {
        return submit_time;
    }

    public void setSubmit_time(long submit_time) {
        this.submit_time = submit_time;
    }

    public long getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(long finish_time) {
        this.finish_time = finish_time;
    }

    public long getCpuHour() {
        return cpuHour;
    }

    public void setCpuHour(long cpuHour) {
        this.cpuHour = cpuHour;
    }

    public long getMap_num() {
        return map_num;
    }

    public void setMap_num(long map_num) {
        this.map_num = map_num;
    }

    public long getReduce_num() {
        return reduce_num;
    }

    public void setReduce_num(long reduce_num) {
        this.reduce_num = reduce_num;
    }

    public long getExecutor_num() {
        return executor_num;
    }

    public void setExecutor_num(long executor_num) {
        this.executor_num = executor_num;
    }

    public long getTask_num() {
        return task_num;
    }

    public void setTask_num(long task_num) {
        this.task_num = task_num;
    }
}
