---
layout: global
title: 数据源（DataSource）管理
description: Dbus Web 数据源管理 BUS_VERSION_SHORT
typora-root-url: ./
---

​	数据源（DataSource）管理界面主要展示数据源有关的一些基本信息，例如数据源名称、类型、状态、url等。其中mysql/Oracle这样的关系型数据库，masterURL和slaveURL不能为空。

​	在数据源（DataSource）管理界面，可以进行数据源基本信息的修改、数据源删除及为数据源添加schema等操作。

## 1  Data Source管理页面入口

​	登录Dbus Web管理系统，Data Source管理页面入口如下图所示：

![](img/config-datasource/config-datasource-1-dsentry.png)

### 1.1 数据源基本信息修改

​	点击目标数据源对应的more/modify按钮，如下图所示： 

![](img/config-datasource/config-datasource-2-modify.png)

​	打开数据源基本信息修改页面，如下图所示。

![](img/config-datasource/config-datasource-3-dsedit.png)

​	输入数据源相关信息，保存即可。

### 1.2 删除数据源

​	点击目标数据源对应的more/delete按钮，如下图所示：

![](img/config-datasource/config-datasource-4-dsdel.png)

​	在弹出的提示框里点击“确定”按钮，即可。如下图所示：

![](img/config-datasource/config-datasource-5-dsdelconfirm.png)

### 1.3 添加Schema

​	参考：[添加Schema和表](config-add-schema-and-table.html)
