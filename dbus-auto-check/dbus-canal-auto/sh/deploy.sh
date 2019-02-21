#!/bin/sh

#获得当前shell所在路径
basepath=$(cd `dirname $0`; pwd)
#echo $basepath
echo "****************************************** starting *****************************************"
#jvm启动参数
#GC_OPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -Xloggc:/data/dbus-canal-auto/logs/gc/gc.log"
LOG_CONF="-Dlogs.base.path=$basepath -Duser.dir=$basepath"
OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$basepath/logs/oom"
JVM_OPTS="-server -Xmx4096m -Xms100m -XX:NewRatio=1"
CLASS_PATH=""
MAIN="com.creditease.dbus.canal.auto.AutoDeployStart"
if [ "x$1" = "xcheck" ]
then
        MAIN="com.creditease.dbus.canal.auto.AutoCheckStart"
fi


#导入jar和config进入classpath
for i in $basepath/lib/*.jar;
        do CLASS_PATH=$i:"$CLASS_PATH";
done
export CLASS_PATH=.:$CLASS_PATH

java $JVM_OPTS $LOG_CONF $OOM_OPTS -classpath $CLASS_PATH $MAIN
sleep 1
cd reports
filename=`ls -ltr |grep canal_| tail -n 1 | awk '{print $9}'`
if [ "x$filename" != "x" ]; then
         cat $filename
         echo "report文件： $filename"
         sleep 1
fi
