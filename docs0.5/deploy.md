---
layout: global
title: 安装部署说明
description: 安装部署说明
---



# 安装部署说明

安装部署分为两种方式，这两种方式不可以混用：

- all in one体验版
  - 该版本 只在单机中安装，自动部署安装dbus所依赖的相关组件，仅用于体验dbus基本功能，不可以用于生产中使用。
  - 该版本包含mysql数据源 和 logstash 日志数据源
  - 参考：link
- 集群部署
  - 用于部署在生产环境或测试环境
  - 可以用于部署集群或单机
  - 该版本包含Oracle、mysql，logstash、filebeat、flume等数据源
  - 内容包括：
    - 基础依赖环境部署， link
      - 这是dbus依赖的基础，必须部署基础环境才能部署mysql的数据源
    - mysql数据源部署说明， link
    - Oracle数据源部署说明， link
    - logstash数据源部署说明， link
    - flume数据源部署说明， link
    - filebeat数据源部署说明， link

