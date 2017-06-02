#!/bin/bash

CASTERLY_HOME=$(dirname $0)/../
CLASSPATH=`hadoop classpath`
CASTERLY_PID=$CASTERLY_HOME/casterly.pid
JAVA=`which java`

startStop=$1

for f in $CASTERLY_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH};
done

CLASSPATH=$CASTERLY_HOME/conf/:$CLASSPATH
JAVA_OPT=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6006


case $startStop in
    (start)
        nohup ${JAVA} -cp $CLASSPATH org.nchc.bigdata.casterly.Casterly > /dev/null 2>&1 &
        echo $! > $CASTERLY_PID
    ;;
    (stop)
        TARGET_PID=`cat $CASTERLY_PID`
        kill $TARGET_PID > /dev/null 2>&1
        rm $CASTERLY_PID
    ;;
    (*)
        echo "./casterly.sh [start|stop]"
        exit 1
    ;;
esac