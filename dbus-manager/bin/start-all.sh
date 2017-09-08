#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
${basepath}/service.sh start
echo "web service started."
${basepath}/manager.sh start
echo "manager started."
${basepath}/httpserver.sh start
echo "http server started."
${basepath}/proxy.sh start
echo "proxy server started."
