package org.nchc.bigdata.casterly;


/**
 * Created by 1403035 on 2016/2/22.
 */
public class Const {
    public static final String SQL_USER = "SQL_USER";
    public static final String SQL_PASSWORD = "SQL_PASSWORD";
    public static final String SQL_URL = "SQL_URL";

    public static final String SQL_TEMPLATE_ADD_JOB = "INSERT INTO " +
            "APP_SUMMARY " +
            "(EPOCH, SEQ, USER, JOBNAME, QUEUE, START, FINISH, CPUHOUR) " +
            "VALUES " +
            "(?,?,?,?,?,?,?,?)";

    // Insert Executor info of a Spark application
    public static final String SQL_TEMPLATE_ADD_EXECUTOR = "INSERT INTO "+
            "EXECUTOR_DETAIL" +
            "(EPOCH ,SEQ    ,ID , START)"+
            "VALUES " +
            "(?    ,?     ,?, ?    )";

    // Find out the number of executor of a Spark Application
    public static final String SQL_TEMPLATE_EXECOUNT_COUNT =
            "SELECT COUNT(ID) AS executor_count FROM EXECUTOR_DETAIL " +
                    "WHERE EPOCH = %d AND SEQ = %d";

    // Find out the number of map (reduce) of a MR Application
    public static final String SQL_TEMPLATE_TASK_COUNT =
            "SELECT COUNT(TYPE) AS task_count FROM TASK_DETAIL " +
                    "WHERE EPOCH = %d AND SEQ = %d AND TYPE = '%s'";

    // Find out the CpuHour of a MR/Spark Application
    public static final String SQL_TEMPLATE_USAGE =
            "SELECT CPUHOUR FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";

    // Find out the Accounting data of a MR/Spark Application
    public static final String SQL_TEMPLATE_JOB_SUM =
            "SELECT * FROM APP_SUMMARY WHERE EPOCH = %d AND SEQ = %d";

    // add last processed file timestamp into db
    public static final String SQL_TEMPLATE_UPDATE_LASTPROCESSED =
            "UPDATE LAST_PROCESSED SET LAST = %d WHERE ID = 1" ;

    // Find out the last processed file timestamp
    public static final String SQL_TEMPLATE_GET_LASTPROCESSED =
            "SELECT LAST FROM LAST_PROCESSED WHERE ID = 1";

    // add map or reduce task into DB
    public static final String SQL_TEMPLATE_ADD_TASK =
            "INSERT INTO TASK_DETAIL" +
            "(EPOCH , SEQ   , TYPE  , TASKID, ATTEMPTID , START , FINISH)"+
            "VALUES" +
            "(?    , ?    , ?    , ?    , ?        , ?    , ?)" ;

    public static final String DAO_CLAZZ_SPARK = "org.nchc.bigdata.dao.SparkJobDAOImpl";
    public static final String DAO_CLAZZ_MAPREDUCE = "org.nchc.bigdata.dao.MRJobDAOImpl";

    public static final String FILTER_CLAZZ_SPARK = "org.nchc.bigdata.filter.SparkLogFileFilter";
    public static final String PARSER_CLAZZ_SPARK = "org.nchc.bigdata.parser.SparkLogParserImpl";

    public static final String FILTER_CLAZZ_MAPREDUCE = "org.nchc.bigdata.filter.MRLogFileFilter";
    public static final String PARSER_CLAZZ_MAPREDUCE = "org.nchc.bigdata.parser.MRLogParserImpl";

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
