package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by 1403035 on 2016/2/17.
 */
public class SparkJobDAOImpl extends JobDAO{

    private static Logger logger = Logger.getLogger(SparkJobDAOImpl.class);

    public SparkJobDAOImpl(Configuration conf){
        this.conf = conf;
    }

    public SparkJobDAOImpl(){}

    @Override
    public long calCPUHour(JobModel model) {
        SparkJobModel.AppEnd appEnd = ((SparkJobModel) model).getAppEnd();
        long endTS = appEnd.getTimestamp();
        long totalTime = 0;
        if (model instanceof SparkJobModel){
            List<SparkJobModel.ExecutorAdded> executors = ((SparkJobModel) model).getExecutorAdd();
            for(SparkJobModel.ExecutorAdded executor : executors){
                totalTime = totalTime + (endTS - Long.parseLong(executor.getTime()));
            }
            return totalTime;
        }
        logger.warn("Not SparkJobModel, but calculate CPUHour in SparkJobDAO");
        return 0;
    }


    @Override
    public boolean add(List<JobModel> models) throws SQLException {
        ResultSet rs = null;
        connection = ConnectionFactory.getConnection(conf);
        statement = connection.createStatement();
        String query;
        try {
            for (JobModel model : models) {
                if (!(model instanceof SparkJobModel)) {
                    logger.warn("Not SparkJobModel. Can not add by " + this.getClass().getSimpleName() + " class");
                    return false;
                }

                // add basic Application info
                long epoch = getEpoch(((SparkJobModel) model).getAppStart().getId());
                long seq = getSeq(((SparkJobModel) model).getAppStart().getId());
                long cpuhour = calCPUHour(model);
                String user = ((SparkJobModel) model).getAppStart().getUser();
                String jobName = ((SparkJobModel) model).getAppStart().getName();
                long start = ((SparkJobModel) model).getAppStart().getTimestamp();
                long finish = ((SparkJobModel) model).getAppEnd().getTimestamp();
                query = String.format(Const.SQL_TEMPLATE_ADD_JOB,
                        epoch, seq, user, jobName, "spark", start, finish, cpuhour);
                rs = statement.executeQuery(query);

                // add executor info
                List<SparkJobModel.ExecutorAdded> executorList = ((SparkJobModel) model).getExecutorAdd();
                for(SparkJobModel.ExecutorAdded executor : executorList){
                    long executor_start = Long.parseLong(executor.getTime());
                    int executor_id = Integer.parseInt(executor.getId());
                    query = String.format(Const.SQL_TEMPLATE_ADD_EXECUTOR, epoch,seq,executor_id,executor_start);
                    rs = statement.executeQuery(query);
                }

            }
        }finally{
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return true;
    }

    @Override
    public List<ResponseJobModel> findByName(String name) throws SQLException {

        return null;
    }


    @Override
    public ResponseJobModel findById(long epoch, int seq) throws SQLException{
        ResultSet rs = accessDbById(epoch, seq);
        ResponseJobModel resultModel = new ResponseJobModel();

        try{
            // create ResponseJobModel from query result
            if(rs.next()) {
                resultModel.setCpuHour(rs.getLong("CPUHOUR"));
                resultModel.setUser(rs.getString("USER"));
                resultModel.setQueue(rs.getString("QUEUE"));
                resultModel.setJobName(rs.getString("JOBNAME"));
                resultModel.setSubmit_time(rs.getLong("START"));
                resultModel.setFinish_time(rs.getLong("FINISH"));
                resultModel.setTask_num(0L);
            }
            // count the number of executor
            rs = statement.executeQuery(String.format(Const.SQL_TEMPLATE_EXECOUNT_COUNT, epoch,seq));
            if(rs.next()){
                resultModel.setExecutor_num(rs.getInt("executor_count"));
            }

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return resultModel;
    }

}
