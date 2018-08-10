---
layout: global
title: 快速开始
description: Dbus快速开始手册 DBUS_VERSION_SHORT
---


# 编译源代码

## 1 编译前准备

### 1.1 添加依赖

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

### 1.2 安装node js（如下需要build dbus-web模块）

默认情况是编译 dbus-web的，这就需要node js。

如果不需要编译dbus-web，可以在 根目录中 pom.xml , 将dbus-manager注释掉，如下：

```
<!--          <module>dbus-manager</module>  -->
<!--          <module>dbus-manager-distribution</module>  -->
```

如果需要编译 dbus-web，则需要下载 node js。**推荐下载 node-v6.9.2**

推荐从淘宝npm站点安装

<https://npm.taobao.org/mirrors/node/v6.9.2/node-v6.9.2-x64.msi>

下载后，推荐安装cnpm

```
npm install -g cnpm --registry=https://registry.npm.taobao.org
```



## 2 编译

由于encoder-base和 dbus-commons 两模块被其他模块引用。

推荐直接使用 mave install整个工程



### 安装过程中的常见错误

#### dbus-manger 相关node js错误

这是因为node 下载库出错造成的，解决问题办法是直接进入两个相关的node的目录：

可以删除node_modules中的内容， 直接调用cnpm install

```
cd  .\dbus-manger\web 
cnpm install

cd  .\dbus-manager\manager
cnpm install
```
