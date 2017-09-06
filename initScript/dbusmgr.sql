create database dbusmgr;

CREATE USER dbusmgr IDENTIFIED BY 'jNXbcK&*ms2hmvRI';

GRANT ALL ON dbusmgr.* TO dbusmgr@'%' IDENTIFIED BY 'jNXbcK&*ms2hmvRI';

flush privileges;

USE `dbusmgr`;

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_avro_schema
-- ----------------------------
DROP TABLE IF EXISTS `t_avro_schema`;
CREATE TABLE `t_avro_schema` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_id` int(11) NOT NULL COMMENT 't_dbus_datasource表ID',
  `namespace` varchar(64) NOT NULL DEFAULT '' COMMENT 'schema名字',
  `schema_name` varchar(64) NOT NULL DEFAULT '' COMMENT 'schema名字',
  `full_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'schema名字',
  `schema_hash` int(11) NOT NULL,
  `schema_text` varchar(20460) NOT NULL DEFAULT '' COMMENT 'schema字符串，json',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_schema_name` (`full_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='解析消息的avro schema表';


-- ----------------------------
-- Table structure for t_data_schema
-- ----------------------------
DROP TABLE IF EXISTS `t_data_schema`;
CREATE TABLE `t_data_schema` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_id` int(11) unsigned NOT NULL COMMENT 't_dbus_datasource 表ID',
  `schema_name` varchar(64) NOT NULL DEFAULT '' COMMENT 'schema',
  `status` varchar(32) NOT NULL DEFAULT '' COMMENT '状态 active/inactive',
  `src_topic` varchar(64) NOT NULL DEFAULT '' COMMENT '源topic',
  `target_topic` varchar(64) NOT NULL DEFAULT '' COMMENT '目表topic',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` varchar(128) DEFAULT NULL COMMENT 'schema描述信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_dsid_sname` (`ds_id`,`schema_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_data_tables
-- ----------------------------
DROP TABLE IF EXISTS `t_data_tables`;
CREATE TABLE `t_data_tables` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_id` int(11) unsigned NOT NULL COMMENT 't_dbus_datasource 表ID',
  `schema_id` int(11) unsigned NOT NULL COMMENT 't_tab_schema 表ID',
  `schema_name` varchar(64) NOT NULL DEFAULT '' COMMENT 'schema',
  `table_name` varchar(64) NOT NULL DEFAULT '' COMMENT '表名',
  `physical_table_regex` varchar(96) DEFAULT NULL,
  `output_topic` varchar(96) DEFAULT '' COMMENT 'kafka_topic',
  `ver_id` int(11) unsigned DEFAULT NULL COMMENT '当前使用的meta版本ID',
  `status` varchar(32) NOT NULL DEFAULT 'abort' COMMENT '状态 waiting：等待拉全量 ok:拉全量结束，可以开始增量数据处理 abort：需要抛弃该表的数据',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `meta_change_flg` int(1) DEFAULT '0' COMMENT 'meta变更标识，初始值为：0，表示代表没有发生变更，1：代表meta发生变更。该字段目前mysql appender模块使用。',
  `batch_id`  int(11) NULL DEFAULT 0 COMMENT '批次ID，用来标记拉全量的批次，每次拉全量会++，增量只使用该字段并不修改' ,
  `ver_change_history`  varchar(96) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '表版本变更历史，存储的值为ver_id，格式为 数字,数字,数字,... ' ,
  `ver_change_notice_flg`  int(1) NOT NULL DEFAULT 0 COMMENT '在web上是否提示表结构发生了变更，1为是，0为不是' ,
  `output_before_update_flg`  int(1) NOT NULL DEFAULT 0 COMMENT '指定该表是否在UMS中输出b（before update）' ,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_sid_tabname` (`schema_id`,`table_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_dbus_datasource
-- ----------------------------
DROP TABLE IF EXISTS `t_dbus_datasource`;
CREATE TABLE `t_dbus_datasource` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_name` varchar(64) NOT NULL DEFAULT '' COMMENT '数据源名字',
  `ds_type` varchar(32) NOT NULL DEFAULT '' COMMENT '数据源类型oracle/mysql',
  `status` varchar(32) NOT NULL DEFAULT '' COMMENT '状态：active/inactive',
  `ds_desc` varchar(64) NOT NULL COMMENT '数据源描述',
  `topic` varchar(64) NOT NULL DEFAULT '',
  `ctrl_topic` varchar(64) NOT NULL DEFAULT '',
  `schema_topic` varchar(64) NOT NULL,
  `split_topic` varchar(64) NOT NULL,
  `master_url` varchar(4000) NOT NULL DEFAULT '' COMMENT '主库jdbc连接串',
  `slave_url` varchar(4000) NOT NULL DEFAULT '' COMMENT '备库jdbc连接串',
  `dbus_user` varchar(64) NOT NULL,
  `dbus_pwd` varchar(64) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uidx_ds_name` (`ds_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dbus数据源配置表';

-- ----------------------------
-- Table structure for t_encode_columns
-- ----------------------------
DROP TABLE IF EXISTS `t_encode_columns`;
CREATE TABLE `t_encode_columns` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) DEFAULT NULL COMMENT '表ID',
  `field_name` varchar(64) DEFAULT NULL COMMENT '字段名称',
  `encode_type` varchar(64) DEFAULT NULL COMMENT '脱敏方式',
  `encode_param` varchar(512) DEFAULT NULL COMMENT '脱敏使用的参数',
  `desc_` varchar(64) DEFAULT NULL COMMENT '描述',
  `truncate` int(1) DEFAULT '0' COMMENT '1:当字符串类型字段值脱敏后超出源表字段长度时按照源表字段长度截取 0:不做截取操作',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='脱敏配置表';

-- ----------------------------
-- Table structure for t_meta_version
-- ----------------------------
DROP TABLE IF EXISTS `t_meta_version`;
CREATE TABLE `t_meta_version` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `table_id` int(11) NOT NULL,
  `ds_id` int(11) unsigned NOT NULL COMMENT 't_dbus_datasource表的ID',
  `db_name` varchar(64) NOT NULL DEFAULT '' COMMENT '数据库名',
  `schema_name` varchar(64) NOT NULL DEFAULT '' COMMENT 'schema名',
  `table_name` varchar(64) NOT NULL DEFAULT '' COMMENT '表名',
  `version` int(11) NOT NULL COMMENT '版本号',
  `inner_version` int(11) NOT NULL COMMENT '内部版本号',
  `event_offset` bigint(20) DEFAULT NULL COMMENT '触发version变更的消息在kafka中的offset值',
  `event_pos` bigint(20) DEFAULT NULL COMMENT '触发version变更的消息在trail文件中的pos值',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日期',
  PRIMARY KEY (`id`),
  KEY `idx_event_offset` (`event_offset`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='t_table_meta的版本信息';

-- ----------------------------
-- Table structure for t_storm_topology
-- ----------------------------
DROP TABLE IF EXISTS `t_storm_topology`;
CREATE TABLE `t_storm_topology` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topology_name` varchar(128) NOT NULL,
  `ds_id` int(11) NOT NULL,
  `jar_name` varchar(512) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_table_meta
-- ----------------------------
DROP TABLE IF EXISTS `t_table_meta`;
CREATE TABLE `t_table_meta` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ver_id` int(11) NOT NULL COMMENT '版本id（t_meta_version）',
  `column_name` varchar(64) NOT NULL DEFAULT '' COMMENT '替换特殊字符后生成的列名，替换规则：replaceAll("[^A-Za-z0-9_]", "_")',
  `original_column_name` varchar(64) DEFAULT NULL COMMENT '数据库中原始的列名',
  `column_id` int(4) NOT NULL COMMENT '列ID',
  `internal_column_id` int(11) DEFAULT NULL,
  `hidden_column` varchar(8) DEFAULT NULL,
  `virtual_column` varchar(8) DEFAULT NULL,
  `original_ser` int(11) NOT NULL COMMENT '源表变更序号',
  `data_type` varchar(128) NOT NULL DEFAULT '' COMMENT '数据类型',
  `data_length` int(11) NOT NULL COMMENT '数据长度',
  `data_precision` int(11) DEFAULT NULL COMMENT '数据精度',
  `data_scale` int(11) DEFAULT NULL COMMENT '小数部分长度',
  `char_length` int(11) DEFAULT NULL,
  `char_used` varchar(1) DEFAULT NULL,
  `nullable` varchar(1) NOT NULL DEFAULT '' COMMENT '是否可空Y/N',
  `is_pk` varchar(1) NOT NULL DEFAULT '' COMMENT '是否为主键Y/N',
  `pk_position` int(2) DEFAULT NULL COMMENT '主键的顺序',
  `alter_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建/修改的时间',
  PRIMARY KEY (`id`),
  KEY `idx_table_meta` (`ver_id`,`column_name`,`original_ser`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='抓取的源表数据meta信息';


