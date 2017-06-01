package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.ResponseJobModel;
import org.nchc.bigdata.model.SparkJobModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        long totalTime = 0;
        if (model instanceof SparkJobModel){
            Map<String, SparkJobModel.ExecutorAdded> executorAdds = ((SparkJobModel) model).getExecutorAdd();
            Map<String, SparkJobModel.ExecutorRemoved> executorRemoveds = ((SparkJobModel) model).getExecutorRemoved();

            Iterator<Map.Entry<String, SparkJobModel.ExecutorRemoved>> iter = executorRemoveds.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, SparkJobModel.ExecutorRemoved> entry = iter.next();
                String executor_id = entry.getKey();
                SparkJobModel.ExecutorRemoved executorrRemove = entry.getValue();

                SparkJobModel.ExecutorAdded executorAdd= executorAdds.get(executor_id);

                totalTime = totalTime +
                        Long.parseLong((executorrRemove.getTime())) - Long.parseLong(executorAdd.getTime());
            }
            return totalTime;
        }
        logger.warn("Not SparkJobModel, but calculate CPUHour in SparkJobDAO");
        return 0;
    }


    @Override
    public void add(List<JobModel> models) {
        connection = ConnectionFactory.getConnection(conf);
        try {
            for (JobModel model : models) {
                if (!(model instanceof SparkJobModel)) {
                    logger.warn("Not SparkJobModel. Can not add by " + this.getClass().getSimpleName() + " class");
                }
                String job_id = ((SparkJobModel) model).getAppStart().getId();
                // skip spark job using local mode
                if(job_id.startsWith("local")) {
                    logger.warn("Skip local mode spark Job, JobID: " + job_id);
                    continue;
                }
                // add basic Application info
                long epoch = getEpoch(job_id);
                long seq = getSeq(job_id);
                long cpuhour = calCPUHour(model);
                String user = ((SparkJobModel) model).getAppStart().getUser();
                String jobName = ((SparkJobModel) model).getAppStart().getName();
                long start = ((SparkJobModel) model).getAppStart().getTimestamp();
                long finish = ((SparkJobModel) model).getAppEnd().getTimestamp();

                // add app summary
                addAppSummary(connection, epoch, seq, user, jobName, "spark", start, finish, cpuhour, model);
            }
        } finally{
            DBUtil.close(connection);
        }
    }

    @Override
    public boolean addOtherDetail(JobModel jobModel) {
        // add executor info
        String job_id = ((SparkJobModel) jobModel).getAppStart().getId();
        long epoch = getEpoch(job_id);
        long seq = getSeq(job_id);
        return addExecutorDetail(connection,
                                ((SparkJobModel) jobModel).getExecutorAdd(),
                                ((SparkJobModel) jobModel).getExecutorRemoved(),
                                epoch, seq);
    }

    private boolean addExecutorDetail(Connection connection,
                                      Map<String, SparkJobModel.ExecutorAdded> executorAdd,
                                      Map<String, SparkJobModel.ExecutorRemoved> executorRemoved,
                                      long epoch, long seq){
        PreparedStatement prepStatAddExec;
        try {
            prepStatAddExec = connection.prepareStatement(Const.SQL_TEMPLATE_ADD_EXECUTOR);
        }catch (SQLException sqle){
            logger.error(Util.traceString(sqle));
            return false;
        }

        try{
            Iterator<Map.Entry<String, SparkJobModel.ExecutorRemoved>> iter = executorRemoved.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, SparkJobModel.ExecutorRemoved> entry = iter.next();

                int executor_id = Integer.parseInt(entry.getKey());
                long executor_start = Long.parseLong(executorAdd.get(entry.getKey()).getTime());
                long executor_end = Long.parseLong(entry.getValue().getTime());
                prepStatAddExec.setLong(1, epoch);
                prepStatAddExec.setLong(2, seq);
                prepStatAddExec.setInt(3, executor_id);
                prepStatAddExec.setLong(4, executor_start);
                prepStatAddExec.setLong(5, executor_end);
                prepStatAddExec.executeUpdate();
            }
        }catch (SQLException sqle){
            logger.error(Util.traceString(sqle));
            return false;
        }finally {
            DBUtil.close(prepStatAddExec);
        }
        return true;
    }

    @Override
    public List<ResponseJobModel> findByTime(long start, long end) throws SQLException {

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
