#!/bin/bash
stormPath=$1
topologyType=$2
zkAndPort=$3
dsName=$4
jarPath=$5
projectName=$6
alias=$7

temp=`echo ${jarPath} | grep ".jar"`
if [ -n "$temp" ]; then
    jarName=""
fi

#全量信息
splitterPullerMainClass="com.creditease.dbus.FullPullerTopology"
splitterPullerCmd="${stormPath}/bin/storm jar ${jarPath} ${splitterPullerMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $splitterPullerCmd

#增量信息
dispatcherAppenderMainClass="com.creditease.dbus.stream.DispatcherAppenderTopology"
dispatcherAppenderCmd="${stormPath}/bin/storm jar ${jarPath} ${dispatcherAppenderMainClass} -zk ${zkAndPort} -tid ${dsName}"

#抽取进程信息
extractorMainClass="com.creditease.dbus.extractor.MysqlExtractorTopology"
extractorCmd="${stormPath}/bin/storm jar ${jarPath} ${extractorMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $extractorCmd

#log-processor信息
logProcessorMainClass="com.creditease.dbus.log.processor.DBusLogProcessorTopology"
logProcessorCmd="${stormPath}/bin/storm jar ${jarPath} ${logProcessorMainClass} -zk ${zkAndPort} -tid ${dsName}"
#echo $logProcessorCmd

#router信息
routerMainClass="com.creditease.dbus.router.DBusRouterTopology"
routerCmd="${stormPath}/bin/storm jar ${jarPath} ${routerMainClass} -zk ${zkAndPort} -tid ${dsName} -pn ${projectName} -alias ${alias}"
#echo $routerCmd

#sinker信息
sinkerMainClass="com.creditease.dbus.SinkTopology"
sinkerCmd="${stormPath}/bin/storm jar ${jarPath} ${sinkerMainClass} -zk ${zkAndPort} -tid ${dsName} -sty hdfs"
#echo $sinkerCmd

dispatcherAppender="dispatcher-appender"
splitterPuller="splitter-puller"
extractor="mysql-extractor"
logProcessor="log-processor"
router="router"
sinker="sinker"

if test ${topologyType} = ${splitterPuller}
then
	${splitterPullerCmd}
elif test ${topologyType} = ${dispatcherAppender}
then
	${dispatcherAppenderCmd}
elif test ${topologyType} = ${logProcessor}
then
	${logProcessorCmd}
elif test ${topologyType} = ${extractor}
then
	${extractorCmd}
elif test ${topologyType} = ${router}
then
	${routerCmd}
elif test ${topologyType} = ${sinker}
then
	${sinkerCmd}
fi