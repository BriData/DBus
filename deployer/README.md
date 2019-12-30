# 1 编译说明

## 1.1 添加依赖

在dbus-commons/pom.xml 中，需要在占位符 处引入 mysql和 oracle相关包， 占位符如下：

```
<!-- 你需要添加 mysql 依赖在这里 mysql-connector-java -->

<!-- 你需要添加 oracle 依赖在这里 ojdbc14 -->

<!-- 你需要添加 db2 依赖在这里 db2jcc4 -->

```

将占位符所在地方替换为如下依赖即可：

```
<!-- 你需要添加 mysql 依赖在这里 mysql-connector-java -->
<dependency>
	<groupId>mysql</groupId>
	<artifactId>mysql-connector-java</artifactId>
	<version>5.1.35</version>
</dependency>

<!-- 你需要添加 oracle 依赖在这里 ojdbc7 -->
<!--
	dbcp从1.4升级到2.6.0对应的oracle需要升级,否在报以下错误
	Exception in thread "emit-heartbeat-event-xinghuo" java.lang.AbstractMethodError
	  at org.apache.commons.dbcp2.DelegatingConnection.isValid(DelegatingConnection.java:874)
 -->
<!-- mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.2 -Dpackaging=jar -Dfile=ojdbc7-12.1.0.2.jar -DgeneratePom=true -->
<dependency>
	<groupId>com.oracle</groupId>
	<artifactId>ojdbc7</artifactId>
	<version>12.1.0.2</version>
</dependency>

<!-- 你需要添加 db2 依赖在这里 db2jcc4 -->
<!--
Install driver jar to your local repository with the flowing command.
mvn install:install-file -DgroupId=com.ibm.db2.jcc -DartifactId=db2jcc4 -Dversion=4.23.42 -Dpackaging=jar
-Dfile=db2jcc4-4.23.42.jar -DgeneratePom=true
-->
<dependency>
	<groupId>com.ibm.db2.jcc</groupId>
	<artifactId>db2jcc4</artifactId>
	<version>4.23.42</version>
</dependency>


```



## 1.2 编译

dbus-main是总体工程，其中encoder-base、 dbus-commons等一些模块是公共模块，会被其他引用。

所以，**推荐直接在dbus-main工程上使用mave install命令进行编译。**

# 2 打包说明

## 2.2 打包

dbus-main是总体工程，其中encoder-base、 dbus-commons等一些模块是公共模块，会被其他引用。

所以，**推荐直接在dbus-main工程上使用mvn clean package -P release命令进行打包。**

## 2.2 deployer介绍

deployer工程是dbus的集合包，包含dbus所有用到的脚本、配置文件等，该工程打包后在target目录会生成deployer-0.6.0.zip，直接上传到dbusweb部署服务器即可使用。

## 2.2 deployer-0.6.0.zip介绍

```
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

