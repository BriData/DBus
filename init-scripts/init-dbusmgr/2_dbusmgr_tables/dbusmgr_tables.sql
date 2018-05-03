
SET FOREIGN_KEY_CHECKS=0;

USE dbusmgr;

-- ----------------------------
-- Table structure for t_avro_schema
-- ----------------------------
DROP TABLE IF EXISTS `t_avro_schema`;
CREATE TABLE `t_avro_schema` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`ds_id`  int(11) NOT NULL COMMENT 't_dbus_datasource表ID' ,
`namespace`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema名字' ,
`schema_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema名字' ,
`full_name`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema名字' ,
`schema_hash`  int(11) NOT NULL ,
`schema_text`  varchar(20460) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema字符串，json' ,
`create_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`),
UNIQUE INDEX `idx_schema_name` (`full_name`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
COMMENT='解析消息的avro schema表'
AUTO_INCREMENT=312

;

-- ----------------------------
-- Table structure for t_data_schema
-- ----------------------------
DROP TABLE IF EXISTS `t_data_schema`;
CREATE TABLE `t_data_schema` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`ds_id`  int(11) UNSIGNED NOT NULL COMMENT 't_dbus_datasource 表ID' ,
`schema_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema' ,
`status`  varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态 active/inactive' ,
`src_topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源topic' ,
`target_topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目表topic' ,
`create_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间' ,
`description`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'schema描述信息' ,
PRIMARY KEY (`id`),
UNIQUE INDEX `idx_dsid_sname` (`ds_id`, `schema_name`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=604

;

-- ----------------------------
-- Table structure for t_data_tables
-- ----------------------------
DROP TABLE IF EXISTS `t_data_tables`;
CREATE TABLE `t_data_tables` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`ds_id`  int(11) UNSIGNED NOT NULL COMMENT 't_dbus_datasource 表ID' ,
`schema_id`  int(11) UNSIGNED NOT NULL COMMENT 't_tab_schema 表ID' ,
`schema_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`table_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '表名' ,
`physical_table_regex`  varchar(96) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`output_topic`  varchar(96) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT 'kafka_topic' ,
`ver_id`  int(11) UNSIGNED NULL DEFAULT NULL COMMENT '当前使用的meta版本ID' ,
`status`  varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'abort' COMMENT '状态 waiting：等待拉全量 ok:拉全量结束，可以开始增量数据处理 abort：需要抛弃该表的数据' ,
`create_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间' ,
`meta_change_flg`  int(1) NULL DEFAULT 0 COMMENT 'meta变更标识，初始值为：0，表示代表没有发生变更，1：代表meta发生变更。该字段目前mysql appender模块使用。' ,
`batch_id`  int(11) NULL DEFAULT 0 COMMENT '批次ID，用来标记拉全量的批次，每次拉全量会++，增量只使用该字段并不修改' ,
`ver_change_history`  varchar(96) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`ver_change_notice_flg`  int(1) NOT NULL DEFAULT 0 ,
`output_before_update_flg`  int(1) NOT NULL DEFAULT 0 ,
`description`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
PRIMARY KEY (`id`),
UNIQUE INDEX `idx_sid_tabname` (`schema_id`, `table_name`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=1531

;

-- ----------------------------
-- Table structure for t_dbus_datasource
-- ----------------------------
DROP TABLE IF EXISTS `t_dbus_datasource`;
CREATE TABLE `t_dbus_datasource` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`ds_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '数据源名字' ,
`ds_type`  varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '数据源类型oracle/mysql' ,
`status`  varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态：active/inactive' ,
`ds_desc`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '数据源描述' ,
`topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`ctrl_topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`schema_topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`split_topic`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`master_url`  varchar(4000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '主库jdbc连接串' ,
`slave_url`  varchar(4000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '备库jdbc连接串' ,
`dbus_user`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`dbus_pwd`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
`ds_partition`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
PRIMARY KEY (`id`),
UNIQUE INDEX `uidx_ds_name` (`ds_name`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
COMMENT='dbus数据源配置表'
AUTO_INCREMENT=550

;

-- ----------------------------
-- Table structure for t_ddl_event
-- ----------------------------
DROP TABLE IF EXISTS `t_ddl_event`;
CREATE TABLE `t_ddl_event` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`event_id`  int(11) UNSIGNED NULL DEFAULT NULL COMMENT 'oracle源端event表的serno' ,
`ds_id`  int(11) NOT NULL ,
`schema_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`table_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`column_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`ver_id`  int(11) UNSIGNED NOT NULL COMMENT '当前表版本id' ,
`trigger_ver`  int(11) UNSIGNED NULL DEFAULT NULL COMMENT 'oracle源端event表中version' ,
`ddl_type`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ddl`  varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=387

;

-- ----------------------------
-- Table structure for t_encode_columns
-- ----------------------------
DROP TABLE IF EXISTS `t_encode_columns`;
CREATE TABLE `t_encode_columns` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`table_id`  int(11) NULL DEFAULT NULL COMMENT '表ID' ,
`field_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '字段名称' ,
`encode_type`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '脱敏方式' ,
`encode_param`  varchar(4096) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '脱敏使用的参数' ,
`desc_`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述' ,
`truncate`  int(1) NULL DEFAULT 0 COMMENT '1:当字符串类型字段值脱敏后超出源表字段长度时按照源表字段长度截取 0:不做截取操作' ,
`update_time`  datetime NULL DEFAULT NULL COMMENT '更新时间' ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=41

;

-- ----------------------------
-- Table structure for t_fullpull_history
-- ----------------------------
DROP TABLE IF EXISTS `t_fullpull_history`;
CREATE TABLE `t_fullpull_history` (
`id`  bigint(20) NOT NULL ,
`type`  varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'global, indepent, normal' ,
`dsName`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`schemaName`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`tableName`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`version`  int(11) NULL DEFAULT NULL ,
`batch_id`  int(11) NULL DEFAULT NULL ,
`state`  varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'init, splitting, pulling, ending, abort' ,
`error_msg`  varchar(8096) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`init_time`  datetime NULL DEFAULT NULL ,
`start_split_time`  datetime NULL DEFAULT NULL ,
`start_pull_time`  datetime NULL DEFAULT NULL ,
`end_time`  datetime NULL DEFAULT NULL ,
`update_time`  datetime NULL DEFAULT CURRENT_TIMESTAMP ,
`finished_partition_count`  bigint(20) NULL DEFAULT NULL ,
`total_partition_count`  bigint(20) NULL DEFAULT NULL ,
`finished_row_count`  bigint(20) NULL DEFAULT NULL ,
`total_row_count`  bigint(20) NULL DEFAULT NULL ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci

;

-- ----------------------------
-- Table structure for t_meta_version
-- ----------------------------
DROP TABLE IF EXISTS `t_meta_version`;
CREATE TABLE `t_meta_version` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`table_id`  int(11) NOT NULL ,
`ds_id`  int(11) UNSIGNED NOT NULL COMMENT 't_dbus_datasource表的ID' ,
`db_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '数据库名' ,
`schema_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'schema名' ,
`table_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '表名' ,
`version`  int(11) NOT NULL COMMENT '版本号' ,
`inner_version`  int(11) NOT NULL COMMENT '内部版本号' ,
`event_offset`  bigint(20) NULL DEFAULT NULL COMMENT '触发version变更的消息在kafka中的offset值' ,
`event_pos`  bigint(20) NULL DEFAULT NULL COMMENT '触发version变更的消息在trail文件中的pos值' ,
`update_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日期' ,
`comments`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '表注释' ,
PRIMARY KEY (`id`),
INDEX `idx_event_offset` (`event_offset`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
COMMENT='t_table_meta的版本信息'
AUTO_INCREMENT=1558

;

-- ----------------------------
-- Table structure for t_plain_log_rule_group
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_group`;
CREATE TABLE `t_plain_log_rule_group` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`table_id`  int(11) NOT NULL ,
`group_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`status`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
`json_extract_rule_xml_pack`  longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=128

;

-- ----------------------------
-- Table structure for t_plain_log_rule_group_version
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_group_version`;
CREATE TABLE `t_plain_log_rule_group_version` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`table_id`  int(11) NOT NULL ,
`group_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`status`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ver_id`  int(11) NOT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=336

;

-- ----------------------------
-- Table structure for t_plain_log_rule_type
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_type`;
CREATE TABLE `t_plain_log_rule_type` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`rule_type_name`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=6

;

-- ----------------------------
-- Table structure for t_plain_log_rules
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rules`;
CREATE TABLE `t_plain_log_rules` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`group_id`  int(11) NOT NULL ,
`order_id`  int(4) NOT NULL ,
`rule_type_name`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`rule_grammar`  varchar(20480) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=991

;

-- ----------------------------
-- Table structure for t_plain_log_rules_version
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rules_version`;
CREATE TABLE `t_plain_log_rules_version` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`group_id`  int(11) NOT NULL ,
`order_id`  int(4) NOT NULL ,
`rule_type_name`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`rule_grammar`  varchar(20480) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=1005

;

-- ----------------------------
-- Table structure for t_query_rule_group
-- ----------------------------
DROP TABLE IF EXISTS `t_query_rule_group`;
CREATE TABLE `t_query_rule_group` (
`id`  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '表的主键' ,
`table_id`  bigint(20) NULL DEFAULT NULL COMMENT 't_dbus_datasource 表的 id' ,
`ver_id`  bigint(20) NULL DEFAULT NULL ,
`status`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '状态 active 有效 inactive 无效' ,
`description`  varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`query`  varchar(4000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '获取数据的 sql 或者其他查询语句' ,
`time_field`  varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '生成batch的字段' ,
`init_time`  datetime NULL DEFAULT NULL COMMENT '生成 batch 的初始时间' ,
`time_span_ms`  bigint(20) NULL DEFAULT NULL COMMENT '生成 batch 的时间间隔' ,
`correction_ms`  bigint(20) NULL DEFAULT NULL COMMENT '开始时间修正值' ,
`reserve_time_ms`  bigint(20) NULL DEFAULT NULL COMMENT '保留时间毫秒数(系统当前时间 - end_time < reserve_time_ms  的数据不能获取)' ,
`output_style`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '输出json的格式，flatten：一级扁平展开输出，default：一级展开输出' ,
`save_as_cols`  varchar(10240) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`update_time`  timestamp NULL DEFAULT CURRENT_TIMESTAMP ,
PRIMARY KEY (`id`),
INDEX `idx_query_rule_ver_id` (`ver_id`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=2

;

-- ----------------------------
-- Table structure for t_storm_topology
-- ----------------------------
DROP TABLE IF EXISTS `t_storm_topology`;
CREATE TABLE `t_storm_topology` (
`id`  int(11) NOT NULL AUTO_INCREMENT ,
`topology_name`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ds_id`  int(11) NOT NULL ,
`jar_name`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`update_time`  datetime NULL DEFAULT NULL ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
AUTO_INCREMENT=4

;

-- ----------------------------
-- Table structure for t_table_action
-- ----------------------------
DROP TABLE IF EXISTS `t_table_action`;
CREATE TABLE `t_table_action` (
`id`  bigint(20) NOT NULL ,
`table_id`  bigint(20) NULL DEFAULT NULL ,
`order_id`  int(11) NULL DEFAULT NULL ,
`action_source`  varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`action_type`  varchar(60) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`action_value`  varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`commit`  varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
PRIMARY KEY (`id`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci

;

-- ----------------------------
-- Table structure for t_table_meta
-- ----------------------------
DROP TABLE IF EXISTS `t_table_meta`;
CREATE TABLE `t_table_meta` (
`id`  int(11) UNSIGNED NOT NULL AUTO_INCREMENT ,
`ver_id`  int(11) NOT NULL COMMENT '版本id（t_meta_version）' ,
`column_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '替换特殊字符后生成的列名，替换规则：replaceAll(\"[^A-Za-z0-9_]\", \"_\")' ,
`original_column_name`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '数据库中原始的列名' ,
`column_id`  int(4) NOT NULL COMMENT '列ID' ,
`internal_column_id`  int(11) NULL DEFAULT NULL ,
`hidden_column`  varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`virtual_column`  varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`original_ser`  int(11) NOT NULL COMMENT '源表变更序号' ,
`data_type`  varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '数据类型' ,
`data_length`  bigint(20) NOT NULL COMMENT '数据长度' ,
`data_precision`  int(11) NULL DEFAULT NULL COMMENT '数据精度' ,
`data_scale`  int(11) NULL DEFAULT NULL COMMENT '小数部分长度' ,
`char_length`  int(11) NULL DEFAULT NULL ,
`char_used`  varchar(1) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
`nullable`  varchar(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否可空Y/N' ,
`is_pk`  varchar(1) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否为主键Y/N' ,
`pk_position`  int(2) NULL DEFAULT NULL COMMENT '主键的顺序' ,
`alter_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建/修改的时间' ,
`comments`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '列注释' ,
`default_value`  varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
PRIMARY KEY (`id`),
INDEX `idx_table_meta` (`ver_id`, `column_name`, `original_ser`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
COMMENT='源表数据meta信息'
AUTO_INCREMENT=16813

;
