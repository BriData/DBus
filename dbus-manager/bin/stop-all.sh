#!/bin/sh

basepath=$(cd `dirname $0`; pwd)
${basepath}/proxy.sh stop
${basepath}/httpserver.sh stop
${basepath}/manager.sh stop
${basepath}/service.sh stop
