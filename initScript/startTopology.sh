#!/bin/bash

topologyType=$1
zkAndPort=$2
dsName=$3

#全量信息
splitterPullerJarPath="splitter_puller/"
splitterPullerJarInfo="dbus-fullpuller_1.3-0.3.0-jar-with-dependencies.jar"
splitterPullerMainClass="com.creditease.dbus.FullPullerTopology"
splitterPullerCmd="bin/storm jar ${splitterPullerJarPath}${splitterPullerJarInfo} ${splitterPullerMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $splitterPullerCmd

#增量信息
dispatcherAppenderJarPath="stream/"
dispatcherAppenderJarInfo="dbus-stream-main-0.3.0-jar-with-dependencies.jar"
dispatcherAppenderMainClass="com.creditease.dbus.stream.DispatcherAppenderTopology"
dispatcherAppenderCmd="bin/storm jar ${dispatcherAppenderJarPath}${dispatcherAppenderJarInfo} ${dispatcherAppenderMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $dispatcherAppenderCmd


#抽取进程信息
extractorJarPath="extractor/"
extractorJarInfo="dbus-mysql-extractor-0.3.0-jar-with-dependencies.jar"
extractorMainClass="com.creditease.dbus.extractor.MysqlExtractorTopology"
extractorCmd="bin/storm jar ${extractorJarPath}${extractorJarInfo} ${extractorMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $extractorCmd





dispatcherAppender="dispatcher-appender"
splitterPuller="splitter-puller"
extractor="extractor"

if test ${topologyType} = ${splitterPuller}
then
	${splitterPullerCmd} 
elif test ${topologyType} = ${dispatcherAppender}
then
	${dispatcherAppenderCmd}
else
	${extractorCmd}
fi


