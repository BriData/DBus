#!/bin/sh
if [ "x$3" = "xauth" ]; then
    echo "auth topic:"$2" to:"$1
    ssh -p 22 dbus@dbus-n1 /.../kafka-acls.sh --authorizer-properties zookeeper.connect=dbus-n1:2181 --add --allow-principal User:$1 --group=* --operation Read --topic $2
else
    echo "create topic:"$2",auth to:"$1
    ssh -p 22 dbus@dbus-n1 /.../kafka-topics.sh --create --zookeeper dbus-n1:2181/kafka --replication-factor 2 --partitions 1 --topic $2;/.../kafka-acls.sh --authorizer-properties zookeeper.connect=dbus-n1:2181/kafka --add --allow-principal User:$1 --group=* --operation Read --topic $2