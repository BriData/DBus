#!/bin/sh

#获得当前shell所在路径
basepath=$(cd `dirname $0`; pwd)
#echo $basepath

#jvm启动参数
#GC_OPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -Xloggc:/data/dbus-heartbeat-logs/logs/gc/gc.log"
LOG_CONF="-Dlogs.base.path=$basepath -Duser.dir=$basepath"
OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$basepath/logs/oom"
#JVM_OPTS="-server -Xmx4096m -Xms4096m -XX:NewRatio=1"
JVM_OPTS="-server -Xmx512m -Xms512m -XX:NewRatio=1"
CLASS_PATH=""
MAIN=" com.creditease.dbus.heartbeat.start.Start"
if [ "x$1" = "xstop" ] 
then
        MAIN="com.creditease.dbus.heartbeat.start.Stop"
fi


#导入jar和config进入classpath
for i in $basepath/lib/*.jar;
        do CLASS_PATH=$i:"$CLASS_PATH";
done
export CLASS_PATH=.:$CLASS_PATH

echo "******************************************************************************"
echo "*CLASS_PATH: " $CLASS_PATH
echo "*   GC_OPTS: " $GC_OPTS
echo "*  OOM_OPTS: " $OOM_OPTS
echo "*  JVM_OPTS: " $JVM_OPTS
echo "*      MAIN: " $MAIN
echo "*         1: " $1
echo "******************************************************************************"

exec java $JVM_OPTS $LOG_CONF $OOM_OPTS -classpath $CLASS_PATH $MAIN
