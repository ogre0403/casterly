#!/usr/bin/python

from contextlib import closing
import MySQLdb
import datetime
import time
import sys
import os

ROOT_LOG_DIR = "/home/ogre/casterly"

MYSQL_URL="172.16.1.18"
MYSQL_USER="casterly"
MYSQL_PW="casterly@hcgwc112"
MYSQL_DB="casterly"

QueryApp = "SELECT * FROM APP_SUMMARY WHERE FINISH >= %s AND FINISH <= %s"
QueryExecutor = "select count(*) from EXECUTOR_DETAIL  where EPOCH = %s AND SEQ = %s"
QueryString = "select count(*) from TASK_DETAIL where EPOCH = %s AND SEQ = %s AND TYPE = %s"

# users who execute job will not be charge
WhiteList = ["ogre", "waue", "c00ctn00", "n00chl00"]

# generate in checkInput()
MONTH_LOG_DIR = ""
LOG_DAILY_DETAIL = ""
LOG_DAILY_SUMMARY = ""
QUERY_DATE = ""

db_connection = None


def main():
    startts, endts = checkInput()

    # connect to MySQL DB
    global db_connection
    db_connection = MySQLdb.connect(host=MYSQL_URL, user=MYSQL_USER, passwd=MYSQL_PW, db=MYSQL_DB)

    with db_connection:
        outputLog(startts, endts)


def outputLog(start_ts, end_ts):
    writer1 = os.fdopen(os.open(LOG_DAILY_DETAIL, os.O_WRONLY | os.O_CREAT, 0600), 'w')
    writer2 = os.fdopen(os.open(LOG_DAILY_SUMMARY, os.O_WRONLY | os.O_CREAT, 0600), 'w')

    with closing(db_connection.cursor()) as cur1:

        # execute SQL query

        cur1.execute(QueryApp, (start_ts, end_ts))

        count = 0
        total = 0
        for row in cur1:
            output = [None] * 14
            # login-name
            output[0] = row[2]
            if(output[0] in WhiteList):
                break
            # q-name
            output[1] = row[4]
            # job-name
            output[2] = row[3]
            # job-id
            output[3] = "application_" + str(row[0]) + "_" + str(row[1])
            # job-submit-date
            output[4] = getDateString(row[5] / 1000)
            # job-end-date
            output[5] = getDateString(row[6] / 1000)
            # job-type
            output[6] = "P"

            if (row[4] == "mapreduce"):
                output[7], output[8] = queryMapAndReduce(row[0], row[1])
            elif (row[4] == "spark"):
                output[7] = queryExecutor(row[0], row[1])
                output[8] = 0
            else:
                # min-cpu (number of map)
                output[7] = "0"
                # max-cpu (number of reduce)
                output[8] = "0"

            # real-cpu ( set to 1)
            output[9] = "1"
            # waiting-time
            output[10] = "0"
            # wall-clock-time
            output[11] = str(row[7] / 1000)
            # cpu-time
            output[12] = str(row[7] / 1000)
            output[13] = "0"
            # print ":".join(output)
            count = count + 1
            total = total + row[7] / 1000
            writer1.write(":".join(output) + "\n")

        summary = [None] * 5
        summary[0] = QUERY_DATE
        summary[1] = "0"
        summary[2] = "0"
        summary[3] = str(count)
        summary[4] = str(total)
        # print ":".join(summary)
        writer2.write(":".join(summary) + "\n")

    writer1.close()
    writer2.close()


def queryMapAndReduce(epoch, seq):
    with closing(db_connection.cursor()) as cur1:
        cur1.execute(QueryString, (epoch, seq, "M"))
        MC = cur1.fetchone()[0]
        cur1.execute(QueryString, (epoch, seq, "R"))
        RC = cur1.fetchone()[0]
    return str(MC), str(RC),


def queryExecutor(epoch, seq):
    with closing(db_connection.cursor()) as cur1:
        cur1.execute(QueryExecutor, (epoch, seq))
        EC = cur1.fetchone()[0]
    return str(EC)


def checkInput():
    global MONTH_LOG_DIR
    global LOG_DAILY_DETAIL
    global LOG_DAILY_SUMMARY
    global QUERY_DATE

    if (len(sys.argv) == 1):
        # start date is not specified, query yesterday
        end = datetime.date.today()
        start = end - datetime.timedelta(days=1)
    elif (len(sys.argv) == 2):
        # end date is not specified, query start_date and next day
        start = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        end = start + datetime.timedelta(days=1)
    else:
        usage()
        exit()

    QUERY_DATE = start.strftime("%Y/%m/%d")
    MONTH_LOG_DIR = ROOT_LOG_DIR + os.sep + start.strftime("%Y") + os.sep + start.strftime("%m")
    if not os.path.exists(MONTH_LOG_DIR):
        mkdir_p(MONTH_LOG_DIR)
    LOG_DAILY_DETAIL = MONTH_LOG_DIR + os.sep + "dbqacct." + start.strftime("%Y%m%d")
    LOG_DAILY_SUMMARY = MONTH_LOG_DIR + os.sep + "dbqacct.log." + start.strftime("%Y%m%d")
    return getTimestamp(start.strftime('%Y%m%d')), int(getTimestamp(end.strftime('%Y%m%d')) - 1000)


def getDateString(ts):
    return datetime.datetime.fromtimestamp(ts).strftime('%Y/%m/%d %H/%M/%S')


def getTimestamp(YMD):
    a = YMD + " 00:00:00"
    timeArray = time.strptime(a, "%Y%m%d %H:%M:%S")
    return int(time.mktime(timeArray)) * 1000


def usage():
    print "." + os.sep + os.path.basename(__file__) + " [start_date] "


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


if __name__ == "__main__":
    main()
