---
layout: global
title: zookeeper目录结构介绍
description: Dbus Web zk节点介绍 DBUS_VERSION_SHORT
---



ZookeeperManager是Dbus web主目录之一，用来展示dbus系统中存储到中zookeeper的相关信息。dbus的zookeeper中主要有DBus的主目录，其中DBus目录下存放着Dbus相关的配置文件及运行过程中的中间状态。下面重点对DBus目录下面的配置结点进行介绍。

DBus目录下面ConfTemplates存储Extractor和Topology的配置模板信息，新加线时，Dbus系统会从该目录下复制相应的部分，到新的数据线中。HeartBeat结点下记录着Dbus系统的心跳子系统的相关状态信息。Topology结点下记录着Dbus系统的Storm的Topology的配置信息。

## 1 心跳双活

Dbus的心跳配置成双活状态，以保证高可用状态。

![zknode-1-heartbeat_double_alive](img\config-zk-node\zknode-1-heartbeat_double_alive.PNG)

## 2 心跳监控状态

在目录/Dbus/HeartBeat/Monitor下查看某数据表的心跳状态：

​	![zknode_2_heartbeat_monitor](img/config-zk-node/zknode_2_heartbeat_monitor.PNG)

## 3 全量拉取状态

在/Dbus/FullPuller目录下，选择相应的数据源数据表和版本，查看全量拉取状态。

![zknode_3_fullpuller](img/config-zk-node/zknode_3_fullpuller.PNG)

	其中：
		Partitions: 分片数
	    TotalCount : 总分片数
	    FinishedCount : 当前完成分片数
	    TotalRows : 数据总行数
	    FinishedRows : 当前已拉取行数
	    StartSecs : 拉取开始时间
	    ConsumeSecs: 拉取数据已耗时多少
	    CreateTime : 拉取请求开始时间
	    UpdateTime : 更新时间
	    StartTime : 拉取开始时间
	    EndTime: 拉取结束时间
	    ErrorMsg : 拉取过程中产生的错误信息
	    Status :splitting表示正在分片，pulling表示正在拉取，ending表示拉取成功
## 4 Topology配置介绍

在/Dbus/Topology目录存放Storm Topology配置信息，主要包含Appender、Dispacher、Dbus-fulldata-puller、Dbus-fulldata-splitter、log-processor。每个数据源都有这些配置信息。如果要特别的修改Topology的配置信息，可以在Dbus Web的Zookeeper Manager中进行相应的修改，并保存修改。