---
layout: global
title: 脱敏管理说明
description: Dbus keeper脱敏管理说明
---

**脱敏等级：**

- 系统级强制脱敏 : 最高级别的脱敏，配置的强制脱敏或统一要求的脱敏，所有项目都需要遵循的默认脱敏规则。管理员设置设置入口：
   ![encode-sys](img/encode/encode-sys.png)
- 项目级强制脱敏：项目级，应用于项目上。管理员用户添加项目时，配置的项目脱敏，表示该项目下所有项目表都需要遵循的脱敏。管理员设置入口：
   ![encode-project-force](img/encode/encode-project-force.png)
- 项目级别自定义脱敏：topology级。同一张表可被分别放到不同topo上，但一个表在同一个topo只能出现一次。同一张表在不同的topo上可配置不同的脱敏等策略。租户在自己的项目中，添加项目表时，配置的信息，入口

 ![encode-project-user](img/encode/encode-project-user.png)


**三级脱敏联系：**

- 系统级强制脱敏、项目级强制脱敏、项目级自定义脱敏强度依次递减，后者不能与前者冲突，需要在遵循前者的基础上添加。

- 在添加项目级强制脱敏时，只能在没有系统级脱敏的信息的列上添加；

- 添加项目级自定义脱敏时，只能在没有系统级脱敏以及 项目级强制脱敏的列上添加

**脱敏插件管理-管理员：**

**脱敏插件**：脱敏方式以插件的形式存在，每个脱敏插件包可有n个设置脱敏类型。管理员可以针对不同的项目上传不同的脱敏插件包，租户在设置项目级自定义脱敏信息时，根据项目下拥有的脱敏插件包，选择脱敏方式。

 ![encode-plugin-global](img/encode/encode-plugin-global.png)

 上传脱敏插件时可以指定应用范围：
 ![ encode-plugin-add](img/encode/encode-plugin-add.png)

###### ##内置脱敏
```
---Dbus keeper自带一个内置脱敏插件，所有项目可用
  
```

