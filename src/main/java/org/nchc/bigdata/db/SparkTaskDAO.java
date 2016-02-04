package org.nchc.bigdata.db;


import org.apache.hadoop.conf.Configuration;

/**
 * Created by 1403035 on 2016/2/3.
 */
public class SparkTaskDAO extends TaskDAO {
    private SparkJobDAO.TaskStart taskStart = null;
    private SparkJobDAO.TaskEnd taskEnd = null;

    public SparkTaskDAO(Configuration conf){
        SparkTaskDAO.conf = conf;
    }

    public SparkJobDAO.TaskStart getTaskStart(){
        return this.taskStart;
    }

    public SparkJobDAO.TaskEnd getTaskEnd(){
        return this.taskEnd;
    }

    public void setTaskStart(SparkJobDAO.TaskStart start){
        if(taskStart == null)
            this.taskStart = start;
    }

    public void setTaskEnd(SparkJobDAO.TaskEnd end){
        if(taskEnd == null)
            this.taskEnd = end;
    }
}
