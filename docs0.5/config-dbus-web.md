---
layout: global
title: dbusmgr系统配置
description: Dbus Web 基础配置 DBUS_VERSION_SHORT
---

> **dbusmgr系统配置之前，需保证：**
>
> 1. *保证部署web的机器能够免密登录到部署storm nimbus的机器；*
> 2. *全量、增量、抽取进程程序的jar包需放在storm nimbus的目录下。*
>



## 1 基础配置

点击下图中Check_Storm按钮可进行免密登陆的检验。

此部分为一些全局配置信息，如kafka、zk地址、storm启动脚本路径等，与zk manager部分的/DBus/Commons路径下的配置文件相对应global.properties中的配置内容（点击图中所示红框中的配置按钮可进入此页面）
![](img/config-dbus-web-01.png)
​	

​	每次加载进此配置页面时，从相应配置文件中读取信息对每项配置进行初始赋值，若需修改则在此页面进行修改，并点击Save_Global保存，修改信息保存到相应配置文件。

​	配置内容如下：

bootstrap.servers

```
ip1:port,ip2:port,...,ipn:port                		#用于建立与kafka集群连接的host/port组
```

zookeeper

```
ip:port                                              #zookeeper服务器的地址/port组
```

monitor_url

```
http://ip:port/dashboard/db/schema-statistic-board   #实时监控数据的grafana页面的URL
```

stormStartScriptPath

```
ip:port/app/dbus/apache-storm-1.0.2                  #启动storm topology的脚本路径
```

User

```
app                                                  #登录storm机器的用户名
```

## 2 心跳配置
​	此部分为心跳的配置信息，为zk manager部分的/DBus/HeartBeat/Config路径下的配置文件heartbeat_config.json中的内容
![](img/config-dbus-web-02.png)
​	每次加载进此配置页面时，从相应文件中读取信息对每项配置进行初始赋值，若需修改则在此页面进行修改，并点击Save_Global保存，修改信息保存到相应文件。

配置内容如下：

```
heartbeatInterval：插入心跳间隔时间，单位为秒s

checkInterval：心跳超时检查间隔时间，单位为秒s

checkFullPullInterval：拉取全量检查间隔时间，单位为秒s

deleteFullPullOldVersionInterval：删除全量时间间隔时间：h

maxAlarmCnt：最大报警次数

heartBeatTimeout：心跳超时时间，单位为秒ms

heartBeatTimeoutAdditional：对部分schema进行心跳的单独配置，为其指定开始时间，结束时间及心跳超时时间，可添加删减schema

fullPullTimeout：拉取全量超时时间，单位为毫秒ms

alarmTtl：报警超时时间，单位为毫秒ms

lifeInterval：心跳生命周期间隔，单位为秒s

correcteValue：心跳不同服务器时间修正值

fullPullCorrecteValue：拉取全量不同服务器时间修正值

fullPullSliceMaxPending：拉取全量kafka offeset无消费最大消息数

leaderPath：心跳主备选举控制路径

controlPath：心跳控制路径

monitorPath：心跳监控路径

monitorFullPullPath：拉取全量监控路径

excludeSchema：不做监控的schema

checkPointPerHeartBeatCnt：心跳检查点间隔点数

fullPullOldVersionCnt：拉取全量保留版本数

adminSMSNo：管理者短信号码

adminUseSMS：管理者是否是使用短信

adminEmail：管理者邮箱

adminUseEmail：管理者是否使用邮箱

additionalNotify：对部分schema进行心跳的补充配置，为其指定心跳报警发送邮件的email地址及发送短信的电话号码，可添加删减schema
```
