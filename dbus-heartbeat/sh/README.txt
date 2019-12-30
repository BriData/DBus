#start
nohup ./heartbeat.sh >/dev/null 2>&1 &
#stop
ps -ef | grep 'com.creditease.dbus.heartbeat.start.Start' | grep -v grep | awk '{print $2}'| xargs kill -9
