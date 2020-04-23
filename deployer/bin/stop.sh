#!/bin/bash
echo 停止keeper-service...
#jps -l | grep 'keeper-service-0.6.1.jar'| awk '{print $1}'| xargs kill -9
kill -9 `jps | grep "keeper-service-0.6.1.jar" | cut -d " " -f 1`

echo 停止keeper-mgr...
#jps -l | grep 'keeper-mgr-0.6.1.jar'| awk '{print $1}'| xargs kill -9
kill -9 `jps | grep "keeper-mgr-0.6.1.jar" | cut -d " " -f 1`

echo 停止gateway...
#jps -l | grep 'gateway-0.6.1.jar'| awk '{print $1}'| xargs kill -9
kill -9 `jps | grep "gateway-0.6.1.jar" | cut -d " " -f 1`

echo 停止register-server...
#jps -l | grep 'register-server-0.6.1.jar'| awk '{print $1}'| xargs kill -9
kill -9 `jps | grep "register-server-0.6.1.jar" | cut -d " " -f 1`

echo 停止config-server...
#jps -l | grep 'config-server-0.6.1.jar'| awk '{print $1}'| xargs kill -9
kill -9 `jps | grep "config-server-0.6.1.jar" | cut -d " " -f 1`
