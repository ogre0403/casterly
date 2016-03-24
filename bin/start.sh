#!/bin/bash

CASTERLY_HOME=/tmp/casterly
CLASSPATH=`hadoop classpath`

for f in $CASTERLY_HOME/lib/*.jar; do
    CLASSPATH=$f:${CLASSPATH};
done

CLASSPATH=$CASTERLY_HOME/conf/:$CLASSPATH

/opt/java/bin/java -cp $CLASSPATH org.nchc.bigdata.casterly.Casterly
