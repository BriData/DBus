#!/bin/bash


#获得当前shell所在路径
PWD=$(cd `dirname $0`; pwd)
PWD=$PWD/../

#检查主包是否存在
if [ ! -e $PWD/lib/dbus-tools-0.3.0.jar ]; then
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

java -cp $CLASS_PATH com.creditease.dbus.tools.SourceDbAccessHelper testdb "select * from test_table where rownum < 5"


