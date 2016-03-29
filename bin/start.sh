#!/bin/bash

CASTERLY_HOME=/tmp/casterly
CLASSPATH=`hadoop classpath`
JAVA=/opt/java/bin/java

for f in $CASTERLY_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH};
done

CLASSPATH=$CASTERLY_HOME/conf/:$CLASSPATH
JAVA_OPT=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6006

${JAVA} -cp $CLASSPATH org.nchc.bigdata.casterly.Casterly
