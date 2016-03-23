package org.nchc.bigdata.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.nchc.bigdata.casterly.Const;
import org.nchc.bigdata.casterly.Util;
import org.nchc.bigdata.model.JobModel;
import org.nchc.bigdata.model.MRJobModel;
import org.nchc.bigdata.model.ResponseJobModel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Created by 1403035 on 2016/3/10.
 */
public class MRJobDAOImpl extends JobDAO {

    private static Logger logger = Logger.getLogger(MRJobDAOImpl.class);

    public MRJobDAOImpl(Configuration conf){
        this.conf = conf;
    }

    public MRJobDAOImpl(){}

    @Override
    public long calCPUHour(JobModel model) {
        // check if MR job
        if (!(model instanceof MRJobModel)){
            return 0;
        }
        MRJobModel mrJobModel = (MRJobModel)model;

        // calculate map usage
        long mapUsage = calculateUsage(mrJobModel.getMapAttemptStartMap(),
                                        mrJobModel.getMapAttemptFinishMap());
        long reduceUsage = calculateUsage(mrJobModel.getReduceAttemptStartMap(),
                                            mrJobModel.getReduceAttemptFinishMap());

        long am_startTime = mrJobModel.getAmStarted().getStartTime();

        if (mrJobModel.getJobFinished() != null) {
            long am_finish = mrJobModel.getJobFinished().getFinishTime();
            return mapUsage + reduceUsage + (am_finish-am_startTime);
        } else if(mrJobModel.getJobUnsuccessfulCompletion() != null){
            long am_finish = mrJobModel.getJobUnsuccessfulCompletion().getFinishTime();
            return mapUsage + reduceUsage + (am_finish-am_startTime);
        }else{
            return mapUsage + reduceUsage;
        }
    }

    private long calculateUsage(Map<String, MRJobModel.TaskAttemptStarted> startedMap,
                                Map<String, MRJobModel.TaskAttemptCompleted> completedMap){
        long sum = 0;
        for (Map.Entry<String, MRJobModel.TaskAttemptStarted> entry : startedMap.entrySet()){
            String attemptID = entry.getKey();
            long startTime = entry.getValue().getStartTime();
            long finishTime = completedMap.get(attemptID).getFinishTime();
            sum = sum +(finishTime-startTime);
        }
        return sum;
    }

    @Override
    public boolean add(List<JobModel> models) throws SQLException {
        connection = ConnectionFactory.getConnection(conf);
        MRJobModel mrJobModel;
        boolean isAddAppOK = false;
        boolean isAddMapOK = false;
        boolean isAddReduceOK = false;
        try {
            for (JobModel model : models) {
                if (!(model instanceof MRJobModel)) {
                    logger.warn("Not SparkJobModel. Can not add by "
                            + this.getClass().getSimpleName() + " class");
                    return false;
                }
                mrJobModel = (MRJobModel) model;
                // add basic Application info
                long epoch = getEpoch(mrJobModel.getJobSubmitted().getJobid());
                long seq = getSeq(mrJobModel.getJobSubmitted().getJobid());
                long cpuhour = calCPUHour(model);
                String user = mrJobModel.getJobSubmitted().getUserName();
                String jobName = mrJobModel.getJobSubmitted().getJobName();
                long start = mrJobModel.getAmStarted().getStartTime();
                long finish ;
                if (mrJobModel.getJobFinished() != null) {
                    finish = mrJobModel.getJobFinished().getFinishTime();
                }else{
                    finish = mrJobModel.getJobUnsuccessfulCompletion().getFinishTime();
                }

                // save app aummary to DB
                isAddAppOK = addAppSummary(connection, epoch, seq, user, jobName,
                        start, finish, cpuhour);

                // save Map task attempt detail to DB
                isAddMapOK = addTaskDetail(mrJobModel.getMapAttemptStartMap(),
                        mrJobModel.getMapAttemptFinishMap(),
                        connection,
                        epoch,  seq,   "M");

                // save Reduce task attempt detail to DB
                isAddReduceOK = addTaskDetail(mrJobModel.getReduceAttemptStartMap(),
                        mrJobModel.getReduceAttemptFinishMap(),
                        connection,
                        epoch, seq, "R");
            }
        }finally{
            DBUtil.close(connection);
        }
        return isAddAppOK && isAddMapOK && isAddReduceOK;
    }

    private boolean addAppSummary(Connection connection,
                                  long epoch, long seq,
                                  String user, String jobName,
                                  long start, long finish, long cpuhour){
        ResultSet rs = null;
        Statement statement;

        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error(Util.traceString(e));
            return false;
        }

        String query = String.format(Const.SQL_TEMPLATE_ADD_JOB,
                epoch, seq, user, jobName, "mapreduce", start, finish, cpuhour);
        try {
            rs = statement.executeQuery(query);
        }catch (SQLException e) {
            logger.error(Util.traceString(e));
            return false;
        } finally {
            // close local variable, global variable connection SHOULD NOT BE CLOSED HERE.
            DBUtil.close(rs);
            DBUtil.close(statement);
        }
        return true;
    }

    private boolean addTaskDetail(Map<String, MRJobModel.TaskAttemptStarted> startedMap,
                                  Map<String, MRJobModel.TaskAttemptCompleted> completedMap,
                                  Connection connection,
                                  long epoch,
                                  long seq,
                                  String type) {
        ResultSet rs = null;
        Statement statement;

        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error(Util.traceString(e));
            return false;
        }

        try {
            for (Map.Entry<String, MRJobModel.TaskAttemptStarted> entry : startedMap.entrySet()){
                String attemptID = entry.getKey();
                long startTime = entry.getValue().getStartTime();
                long finishTime = completedMap.get(attemptID).getFinishTime();
                long taskid = Long.parseLong(attemptID.split("_")[0]);
                long attemptid = Long.parseLong(attemptID.split("_")[1]);
                String query = String.format(Const.SQL_TEMPLATE_ADD_TASK,
                        epoch, seq, type, taskid, attemptid, startTime, finishTime);
                rs = statement.executeQuery(query);
            }
        } catch (SQLException e) {
            logger.error(Util.traceString(e));
            return false;
        } finally {
            // close local variable, global variable connection SHOULD NOT BE CLOSED HERE.
            DBUtil.close(rs);
            DBUtil.close(statement);
        }
        return true;
    }

    @Override
    public List<ResponseJobModel> findByTime(long start, long end) throws SQLException {
        return null;
    }

    @Override
    public ResponseJobModel findById(long epoch, int seq) throws SQLException {
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
            }
            // count the number of map and reduce task attempt
            resultModel.setMap_num(findTaskNum(epoch,seq,"M"));
            resultModel.setReduce_num(findTaskNum(epoch,seq,"R"));

        }finally {
            DBUtil.close(rs);
            DBUtil.close(statement);
            DBUtil.close(connection);
        }
        return resultModel;
    }

    @Override
    public void close() {
        //TODO
    }

    private long findTaskNum(long epoch, int seq, String type) {
        ResultSet rs = null;

        try {
            rs = statement.executeQuery(
                    String.format(Const.SQL_TEMPLATE_TASK_COUNT, epoch, seq, type));
            return (rs.next()) ? rs.getLong("task_count") : 0L;
        }catch (SQLException e){
            logger.warn(Util.traceString(e));
            return 0L;
        }finally {
            DBUtil.close(rs);
        }
    }
}
