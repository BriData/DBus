---
layout: global
title: 安装部署
description: 安装部署说明
---
* This will become a table of contents (this text will be scraped).
{:toc}

# 1 安装部署说明

安装部署分为以下两种方式（注意：两种方式不可以混用）：

- **All In One体验版(已废弃，建议参考集群部署)**
  - 该版本安装在单机上，自动部署安装dbus所依赖的相关组件，**仅用于体验dbus基本功能**，不可以用于生产。
  - 该版本支持mysql数据源接入体验。
- **集群部署**
  - 用于生产环境或测试环境部署。
  - 可部署在单机上或集群上。
  - 该版本包含对Oracle、mysql，logstash、filebeat、flume等数据源的支持。

# 2 快速体验 - All In One包部署

请参考[All In One包快速安装](quick-start.html)。



# 3 集群部署

## 3.1 环境准备

### 3.1.1 硬件基础

Dbus集群环境最少需要三台Linux服务器，以下以三台服务器为例进行说明：

| No   | IP          | 域名      | 运行环境   |
| ---- | ----------- | ------- | ------ |
| 1    | 192.168.0.1 | dbus-n1 | JDK1.8 |
| 2    | 192.168.0.2 | dbus-n2 | JDK1.8 |
| 3    | 192.168.0.3 | dbus-n3 | JDK1.8 |

****

**HOST配置**：修改所有服务器/etc/hosts文件设置相应的域名信息如下：

```
192.168.0.1 dbus-n1
192.168.0.2 dbus-n2
192.168.0.3 dbus-n3
```

**SSH免密登录配置**：配通dbus-n3到dbus-n1、dbus-n2、dbus-n3之间的SSH免密登录。

### 3.1.2 软件依赖

| 名称          | 版本号   | 说明                                                         |
| ------------- | -------- | ------------------------------------------------------------ |
| Zookeeper     | v3.4.6+  | 用于构建整个系统和提供配置通知等。推荐版本：v3.4.8           |
| Kafka         | v0.10    | 用于存储相关数据和消息，提供订阅和发布的能力                 |
| Storm         | v1.0.2   | 用于提供DBus流式计算                                         |
| Influxdb      | v1.1.0   | 用于记录实时监控数据。                                       |
| Grafana       | v4.2.0   | 用于展示监控信息。                                           |
| MySql         | v5.6.x   | 创建数据库dbusmgr。**创建好账号。后续配置需提供。**          |
| Nginx         | v1.9.3   | 用于存放静态html、js文件及反向代理。                         |
| kafka-manager | v1.3.3.4 | **选装**。用于便捷地查看、管理Kafka集群。建议安装。          |
| Canal         | v1.0.22  | DBus用于实时抽取binlog日志。DBus修改一个文件, 具体配置可参考canal相关支持说明，支持mysql5.6，5.7 |

### 3.1.3 推荐部署说明

```
zookeeper：     推荐部署dbus-n1、dbus-n2、dbus-n3。
Storm：         推荐部署dbus-n1、dbus-n2、dbus-n3。
Storm Nimbus：  推荐部署dbus-n1。
Storm UI：      推荐部署dbus-n1。
Kafka：         推荐部署dbus-n1、dbus-n2、dbus-n3。
DBUS Keeper：   推荐部署dbus-n3（若部署集群，可部署到dbus-n2、dbus-n3）。
DBUS HeartBeat：推荐部署dbus-n2、dbus-n3。
```

有关上述基础组件的配置，可参考：[基础组件安装配置](install-base-components.html)

### 3.1.4 前期准备 

#### 3.1.4.1 生成GrafanaToken

 Dbus使用Grafana展示数据线监控信息。需要提供Grafana Token进行监控模板的初始化。

**1** 点击打开API Keys管理页面。  

![grafana-token-01](img/install-base-components/grafana-token-01.png)

**2** 添加Key，名字随意，角色必须是Admin。

![grafana-token-02](img/install-base-components/grafana-token-02.png)

**3** 在跳出来的页面拷贝Key，并保存好。

![grafana-token-03](img/install-base-components/grafana-token-03.png)

## 3.2 DBus安装配置

### 3.2.1 下载Dbus-Keeper

访问[Release Downloads](https://github.com/BriData/DBus/releases)，到该Release页面提供的云盘地址下载 deployer-0.6.0.zip 压缩包，上传到你指定的服务器，解压 unzip deployer-0.6.0.zip。

```
解压后目录：
bin------------------------------dbus的各种执行命令
  |--addTopicAcl.sh            --kerberos环境topic授权脚本
  |--dbus_startTopology.sh     --storm worker启动脚本
  |--init-all.sh               --初始化dbus，包含配置校验、jar包初始化、数据库初始化、dbus其他模块初始化、启动
  |--init-dbus-modules.sh      --dbus其他模块初始化，包含zk节点、grafana、influxdb、
  |--init-jars.sh              --jar包初始化
  |--start.sh                  --启动
  |--stop.sh                   --停止
lib------------------------------dbus-keeper相关jar包
  |--config-server-0.6.0.jar   --dbus配置中心
  |--gateway-0.6.0.jar         --dbus网关
  |--keeper-auto-deploy-0.6.0-jar-with-dependencies.jar--dbus自动部署包
  |--keeper-mgr-0.6.0.jar      --dbus控制台
  |--keeper-service-0.6.0.jar  --dbus数据库服务
  |--register-server-0.6.0.jar --dbus配置中心
conf-----------------------------dbus的配置文件和模板文件
  |--Commons                   --zk节点模板
  |--config.properties         --zk初始化配置文件，后面会用到
  |--nginx.conf                --nginx配置文件
  |--ConfTemplates             --zk节点模板
  |--HeartBeat                 --zk节点模板
  |--init                      --dbus初始化相关脚本
  |--Keeper                    --zk节点模板
  |--keeperConfig              --dbus配置中心配置文件目录
  |--keeperConfigTemplates     --dbus配置中心配置文件模板
  |--worker.xml                --storm log4j配置文件
extlib---------------------------dbus各个模块storm程序包
  |--dbus-fullpuller-0.6.0-jar-with-dependencies.jar      --全量程序包
  |--dbus-log-processor-0.6.0-jar-with-dependencies.jar   --日志程序包
  |--dbus-mysql-extractor-0.6.0-jar-with-dependencies.jar --mysql抽取程序包
  |--dbus-router-0.6.0-jar-with-dependencies.jar          --router程序包
  |--dbus-sinker-0.6.0-jar-with-dependencies.jar          --sinker程序包
  |--dbus-stream-main-0.6.0-jar-with-dependencies.jar     --增量程序包（dispacher-appender）
  |--encoder-plugins-0.6.0.jar                            --脱敏包
zip------------------------------dbus其他模块程序包
  |--dbus-canal-auto-0.6.0.zip --canal自动部署包
  |--dbus-heartbeat-0.6.0.zip  --心跳程序包
  |--dbus-ogg-auto-0.6.0.zip   --ogg自动部署包
  |--log-auto-check-0.6.0.zip  --log自动部署包
  |--build.zip                 --前端js包
  |--canal.zip                 --修改后的canal包1.0.24
```



### 3.2.2  Nginx配置

复制**conf/nginx.conf **到**nginx/conf/** 下替换默认配置文件。

![nginx_conf](img/install-base-components/nginx_conf.png)

复制**zip/build.zip** 到**nginx/html/ **下解压(unzip build.zip)。

```
# 重新加载nginx
../sbin/nginx -s reload
```

![nginx_html_build](img/install-base-components/nginx_html_build.png)



### 3.2.3 修改Dbus-Keeper启动配置

修改dbus配置文件**conf/config.properties**，提供dbus各个模块初始化启动参数

```
#########################################################################################
# 是否使用dbus提供的配置中心,默认开启,目前仅支持spring cloud配置中心
config.server.enabled=true
# 如果config.server.enabled=false,请配置以下地址
spring.cloud.config.profile=环境名必须修改 release
spring.cloud.config.label=分支名必须修改 master
spring.cloud.config.uri=必须修改 http://localhost:19090
# 配置中心端口,端口号可用则需修改
config.server.port=19090

#########################################################################################
# 是否使用dbus提供的注册中心,默认开启
register.server.enabled=true
# 如果register.server.enabled=false,请配置以下地址
register.server.url=必须修改 http://localhost:9090/eureka/
# 注册中心端口,端口号可用则需修改
register.server.port=9090

#########################################################################################
# 暂不支持使用自己的网关
# 网关端口,端口号可用则需修改,如果修改了这个端口号,需要修改3.2.2nginx.config的代理端口配置
gateway.server.port=5090
# dbus控制台端口,端口号可用则需修改
mgr.server.port=8901
# dbus数据库服务端口,端口号可用则需修改
service.server.port=18901
# mysql管理库相关配置
spring.datasource.url=必须修改 jdbc:mysql://mysql_server_ip:3306/dbusmgr?characterEncoding=utf-8
spring.datasource.username=必须修改 dbusmgr
spring.datasource.password=必须修改 dbusmgr!@#123

#########################################################################################
# dbus集群列表,dbus web所在机器必须能免密访问该列表所有机器
dbus.cluster.server.list=必须修改 dbus-n1,dbus-n2,dbus-n3 ....
# dbus集群统一免密用户
dbus.cluster.server.ssh.user=必须修改 例如:app
# dbus集群统一免密端口号
dbus.cluster.server.ssh.port=必须修改 默认22
# ZK地址
zk.str=必须修改 zk_server_ip1:2181,zk_server_ip2:2181,zk_server_ip3:2181
# kafka地址
bootstrap.servers=必须修改 kafka_server_ip1:9092,kafka_server_ip2:9092,kafka_server_ip3:9092
bootstrap.servers.version=0.10.0.0
# influxdb外网地址(域名)
influxdb.web.url=必须修改 http://influxdb_domain_name
# influxdb内网地址,不区分内外网influxdb_url_web和influxdb_url_dbus配置一样即可
influxdb.dbus.url=必须修改 http://influxdb_server_ip:8086
# grafana外网地址(域名)
grafana.web.url=必须修改 http://grafana_domain_name
# grafana内网地址,不区分内外网igrafana_url_web和grafana_url_dbus配置一样即可
grafana.dbus.url=必须修改 http://grafana_server_ip:3000
# grafana管理员token
grafana.token=必须修改 eyJrIjoianQyVjlGdDhhejBtcElCMzhtZzE2eTBpTG1mR1dHV3kiLCJuIjoiYWRtaW4iLCJpZCI6MX0=
# storm nimbus所在机器
storm.nimbus.host=必须修改 storm_nimbus_server_ip
# storm nimbus根目录
storm.nimbus.home.path=必须修改 /app/dbus/apache-storm-1.1.0
# storm worker日志根目录,默认storm.nimbus.home.path下的logs目录
storm.nimbus.log.path=必须修改 /app/dbus/apache-storm-1.1.0/logs
# stormUI url
storm.rest.url=必须修改 http://storm_ui_server_ip:8080/api/v1
# storm在zookeeper的根节点
storm.zookeeper.root=必须修改 /storm
# 心跳程序自动部署目标机器,多个机器逗号隔开(半角逗号)
heartbeat.host=必须修改 heartbeat_server_ip1,heartbeat_server_ip2
# 心跳程序自动部署根目录
heartbeat.path=必须修改 /app/dbus/heartbeat

#########################################################################################
# nginx所在机器ip
nginx.ip=必须修改 nginx_server_ip
# nginx.config配置的listen端口号
nginx.port=必须修改 nginx_server_port
```

**关于配置参数说明：**

storm.nimbus.home.path：这个目录就是storm nimbus安装的根目录；

storm.nimbus.log.path：这个目录默认是storm nimbus安装的根目录下的logs目录，但是如果采用ambari安装或者指定了日志目录，需要填写指定的日志目录；

storm.zookeeper.root：这个是storm.ymal配置文件的storm.zookeeper.root的值

### 3.2.4 启动/停止DBus-Keeper

1、适用于首次使用dbus，包含配置校验、jar包初始化、数据库初始化、启动web、dbus其他模块初始化。**！！！**（该指令执行成功建议删除该命令，该命令包含**重置数据库**，再次执行该命令会丢失全部数据库数据，慎重操作）

```
 ./init-all.sh 
```

2、适用于lib目录下jar包和心跳包初始化。**1 命令执行成功不需要再次执行该命令。**

```
./init-jars.sh 
```

3、适用于dbus web启动成功后，初始化其他模块失败，执行该命令，包含zk基础节点创建、心跳包自动部署、storm程序包   脱敏包自动上传、默认sink添加、influxdb初始化、grafana初始化。**1 命令执行成功不需要再次执行该命令。**

```
./init-dbus-modules.sh
```

4、启动dbus web程序. 

```
./start.sh
```

 5、停止dbus web程序.

```
./stop.sh
```

### 3.2.5 访问dbus

http://nginx_server_ip:nginx_server_port/login

管理员初始账号/密码：admin/12345678

### 3.2.6 初始化常见问题

1、如果发现初始化心跳后没有进程，请检查心跳部署服务器是否安装了unzip命令，初始化需要unzip命令支持。
2、初始化过程中，如果某个环节连通性检测失败，请根据错误提示修配置信息，一般都是ssh免密配置不到位或者初始化参数填写错误。
3、如果确认配置没有问题，仍然初始化失败，请查看后台日志进行诊断（logs/mgr.log和logs/service.log）。
4、web程序启动成功即可登录，登录后可进行其他模块的单独初始化，位置：配置中心/全局配置
6、如果是storm启动出错，检查下storm_env.ini、storm_env.sh的JAVA_HOME是否配置到位。
  在~/.bashrc文件配置JAVA_HOME环境变量
7、如果dbusweb显示：

```
启动dbusweb程序成功
登陆测试中...
登陆测试中...
登陆测试成功.
{"status":10000,"message":"xxxxxx"}  [status非0,有异常信息]
```

表示dbusweb已经成功启动，只需要根据异常信息，更改相关配置后，执行**./init-dbus-modules.sh**即可

8、管理员初始账号/密码：admin/12345678。

