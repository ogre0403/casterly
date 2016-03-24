#!/bin/bash

CASTERLY_HOME=/tmp/casterly
CLASSPATH=`hadoop classpath`
JAVA=/opt/java/bin/java

for f in $CASTERLY_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH};
done

CLASSPATH=$CASTERLY_HOME/conf/:$CLASSPATH

${JAVA} -cp $CLASSPATH org.nchc.bigdata.casterly.Casterly
