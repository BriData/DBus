#!/bin/bash

#获得当前shell所在路径
PWD=$(cd `dirname $0`; pwd)
PWD=$PWD/../

#检查主包是否存在
if [ ! -e $PWD/lib/dbus-tools-2.0.0.jar ]; then
	echo "please execute shell in its path!"
	exit
fi

#导入jar和config进入classpath
CLASS_PATH=""
for i in $PWD/lib/*.jar;
do
        CLASS_PATH=$i:"$CLASS_PATH";
done
CLASS_PATH=.:$CLASS_PATH




java -cp $CLASS_PATH com.creditease.dbus.tools.KafkaReader --topic=test_topic --offset=$1 --maxlength=$2
