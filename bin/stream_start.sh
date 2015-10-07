#!/bin/bash

echo "------start the server!------"

FWDIR=$(cd `dirname $0`/..; pwd)

JAVA_OPTS="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$FWDIR/logs/gc.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$FWDIR/logs/"

SPARK_HOME=

SPARK_ASSEMBLY_JAR=`ls $SPARK_HOME/lib/*assembly*jar`
for jarFile in `ls $FWDIR/lib/*jar`
do
  CLASSPATH=$CLASSPATH:$jarFile
done

CLASSPATH=$CLASSPATH:$SPARK_ASSEMBLY_JAR:$FWDIR/conf/common.xml:$FWDIR/conf/log4j.properties

nohup java -cp $CLASSPATH com.asiainfo.ocdp.stream.manager.MainFrameManager &>> $FWDIR/logs/MainFrameManager.log&

proc_name="MainFrameManager"
name_suffixx="\>"
ps -ef|grep -i ${proc_name}${name_suffixx}|grep -v "grep"|awk '{print $2}' > pid.log

echo "start the server success ! "
