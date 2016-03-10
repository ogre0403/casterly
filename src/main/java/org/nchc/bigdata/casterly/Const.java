package org.nchc.bigdata.casterly;

import org.apache.avro.Schema;

/**
 * Created by 1403035 on 2016/2/22.
 */
public class Const {
    public static final String SQL_USER = "SQL_USER";
    public static final String SQL_PASSWORD = "SQL_PASSWORD";
    public static final String SQL_URL = "SQL_URL";

    // Insert single Application Accounting data
    public static final String SQL_TEMPLATE_ADD_JOB = "INSERT INTO " +
            "APP_SUMMARY " +
            "(EPOCH, SEQ, USER, JOBNAME, QUEUE, START, FINISH, CPUHOUR) " +
            "VALUES " +
            "(%d, %d,  \'%s\', \'%s\', \'%s\', %d, %d, %d)";

    // Insert Executor info of a Spark application
    public static final String SQL_TEMPLATE_ADD_EXECUTOR = "INSERT INTO "+
            "EXECUTOR_DETAIL" +
            "(EPOCH ,SEQ    ,ID , START)"+
            "VALUES " +
            "(%d    ,%d     ,%d, %d    )";

    // Find out the number of executor of a Spark Application
    public static final String SQL_TEMPLATE_EXECOUNT_COUNT =
            "SELECT COUNT(ID) AS executor_count FROM EXECUTOR_DETAIL WHERE EPOCH = %d AND SEQ = %d";

    // Find out the CpuHour of a MR/Spark Application
    public static final String SQL_TEMPLATE_USAGE = "SELECT CPUHOUR FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";

    // Find out the Accounting data of a MR/Spark Application
    public static final String SQL_TEMPLATE_JOB_SUM = "SELECT * FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";

    public static final String SQL_TEMPLATE_UPDATE_LASTPROCESSED =
            "UPDATE LAST_PROCESSED SET LAST = %d WHERE ID = 1" ;

    // Find out the last processed file timestamp
    public static final String SQL_TEMPLATE_GET_LASTPROCESSED =
            "SELECT LAST FROM LAST_PROCESSED WHERE ID = 1";

    public static final String DAO_CLAZZ_SPARK = "org.nchc.bigdata.dao.SparkJobDAOImpl";
    public static final String DAO_CLAZZ_MAPREDUCE = "org.nchc.bigdata.dao.MRJobDAOImpl";



    public final static String JOB_SUBMITTED = "JOB_SUBMITTED";
    public final static String JOB_FINISHED = "JOB_FINISHED";
    public final static String JOB_FAILED = "JOB_FAILED";
    public final static String JOB_KILLED = "JOB_KILLED";
    public final static String JOB_ERROR="JOB_ERROR";
    public final static String MAP_ATTEMPT_STARTED = "MAP_ATTEMPT_STARTED";
    public final static String MAP_ATTEMPT_FINISHED = "MAP_ATTEMPT_FINISHED";
    public final static String MAP_ATTEMPT_FAILED = "MAP_ATTEMPT_FAILED";
    public final static String MAP_ATTEMPT_KILLED = "MAP_ATTEMPT_KILLED";
    public final static String REDUCE_ATTEMPT_STARTED = "REDUCE_ATTEMPT_STARTED";
    public final static String REDUCE_ATTEMPT_FINISHED = "REDUCE_ATTEMPT_FINISHED";
    public final static String REDUCE_ATTEMPT_FAILED = "REDUCE_ATTEMPT_FAILED";
    public final static String REDUCE_ATTEMPT_KILLED = "REDUCE_ATTEMPT_KILLED";
    public final static String AM_STARTED = "AM_STARTED";



}
