#!/bin/sh

#获得当前shell所在路径
base_home=$(cd `dirname $0`; pwd)
pid=${base_home}/.pids/proxy.pid
log=${base_home}/logs/proxy.log
JAVA=java
log_conf="-Dlogs.base.path=$base_home/logs -Duser.dir=$base_home"
oom_opts="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/dbus/logs/oom"
jvm_opts="-server -Xmx512m -Xms512m -XX:NewRatio=1"

class_path=""
echo $cmd
for i in $base_home/lib/*.jar;
    do class_path=$i:"$class_path";
done

main="com.creditease.dbus.http.HttpProxy"
cmd="$JAVA $jvm_opts $log_conf $oom_opts -classpath $class_path $main"

#export CLASS_PATH=.:$class_path

start() {
    if [ ! -e "$base_home/.pids" ] ; then
        mkdir "$base_home/.pids"
    fi
    if [ ! -e "$base_home/logs" ] ; then
        mkdir "$base_home/logs"
    fi
    if [ ! -e "$log" ] ; then
        touch "$log"
    fi
    touch $pid
    if nohup $cmd >>$log 2>&1 &
    then echo $! >$pid
         echo "$(date '+%Y-%m-%d %X'): START" >>$log
    else echo "Error... "
         /bin/rm $pid
    fi
}

stop() {
    [ -f $pid ] && kill `cat $pid` && rm -rf $pid
}

case "$1" in
    'start')
        start
        ;;
    'stop')
        stop
        ;;
    *)
    echo "usage: $0 {start|stop}"
    exit 1
        ;;
esac
