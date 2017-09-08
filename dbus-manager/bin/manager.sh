#!/bin/sh

#获得当前shell所在路径
base_home=$(cd `dirname $0`; pwd)
pid=${base_home}/.pids/manager.pid
log=${base_home}/logs/manager.log

cmd="pm2 start ${base_home}/manager/bin/www"

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
    pm2 stop ${base_home}/manager/bin/www
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
