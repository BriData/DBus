---
layout: global
title: 编译源代码
description: 编译源代码 DBUS_VERSION_SHORT
---

## 1 添加依赖

在dbus-commons/pom.xml 中，需要在占位符 处引入 mysql和 oracle相关包， 占位符如下：

```
<!-- 你需要添加 mysql 依赖在这里 mysql-connector-java -->

<!-- 你需要添加 oracle 依赖在这里 ojdbc14 -->
```

将占位符所在地方替换为如下依赖即可：

```
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.35</version>
</dependency>

<!--
Install driver jar to your local repository with the flowing command.
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc14 -Dversion=10.2.0.2.0 -Dpackaging=jar -Dfile=ojdbc14-10.2.0.2.0.jar -DgeneratePom=true
-->
<dependency>
    <groupId>com.oracle</groupId>
    <artifactId>ojdbc14</artifactId>
    <version>10.2.0.2.0</version>
</dependency>
```



## 2 编译

dbus-main是总体工程，其中encoder-base、 dbus-commons等一些模块是公共模块，会被其他引用。

所以，**推荐直接在dbus-main工程上使用mave install命令进行编译。**
