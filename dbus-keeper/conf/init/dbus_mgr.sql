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
  `schema_text` text NOT NULL COMMENT 'schema字符串，json',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_schema_name` (`full_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='解析消息的avro schema表';

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
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` varchar(128) DEFAULT NULL COMMENT 'schema描述信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_dsid_sname` (`ds_id`,`schema_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_data_tables
-- ----------------------------
DROP TABLE IF EXISTS `t_data_tables`;
CREATE TABLE `t_data_tables` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_id` int(11) unsigned NOT NULL COMMENT 't_dbus_datasource 表ID',
  `schema_id` int(11) unsigned NOT NULL COMMENT 't_tab_schema 表ID',
  `schema_name` varchar(64) DEFAULT NULL,
  `table_name` varchar(64) NOT NULL DEFAULT '' COMMENT '表名',
  `table_name_alias` varchar(64) NOT NULL DEFAULT '' COMMENT '别名',
  `physical_table_regex` varchar(96) DEFAULT NULL,
  `output_topic` varchar(96) DEFAULT '' COMMENT 'kafka_topic',
  `ver_id` int(11) unsigned DEFAULT NULL COMMENT '当前使用的meta版本ID',
  `status` varchar(32) NOT NULL DEFAULT 'abort' COMMENT 'ok,abort,inactive,waiting\r\nok:正常使用;abort:需要抛弃该表的数据;waiting:等待拉全量;inactive:不可用 ',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `meta_change_flg` int(1) DEFAULT '0' COMMENT 'meta变更标识，初始值为：0，表示代表没有发生变更，1：代表meta发生变更。该字段目前mysql appender模块使用。',
  `batch_id` int(11) DEFAULT '0' COMMENT '批次ID，用来标记拉全量的批次，每次拉全量会++，增量只使用该字段并不修改',
  `ver_change_history` varchar(128) DEFAULT NULL,
  `ver_change_notice_flg` int(1) NOT NULL DEFAULT '0',
  `output_before_update_flg` int(1) NOT NULL DEFAULT '0',
  `description` varchar(128) DEFAULT NULL,
  `fullpull_col` varchar(255) DEFAULT '' COMMENT '全量分片列:配置column名称',
  `fullpull_split_shard_size` varchar(255) DEFAULT '' COMMENT '全量分片大小配置:配置-1代表不分片',
  `fullpull_split_style` varchar(255) DEFAULT '' COMMENT '全量分片类型:MD5',
  `is_open` int(1) DEFAULT '0' COMMENT 'mongo是否展开节点,0不展开,1一级展开',
  `is_auto_complete` tinyint(4) DEFAULT '0' COMMENT 'mongoDB的表是否补全数据；如果开启，增量中更新操作会回查并补全数据',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_sid_tabname` (`schema_id`,`table_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_dba_encode_columns
-- ----------------------------
DROP TABLE IF EXISTS `t_dba_encode_columns`;
CREATE TABLE `t_dba_encode_columns` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) DEFAULT NULL COMMENT '表ID',
  `field_name` varchar(64) DEFAULT NULL COMMENT '字段名称',
  `plugin_id` int(11) DEFAULT NULL COMMENT '脱敏插件ID',
  `encode_type` varchar(64) DEFAULT NULL COMMENT '脱敏方式',
  `encode_param` varchar(4096) DEFAULT NULL COMMENT '脱敏使用的参数',
  `desc_` varchar(64) DEFAULT NULL COMMENT '描述',
  `truncate` int(1) DEFAULT '0' COMMENT '1:当字符串类型字段值脱敏后超出源表字段长度时按照源表字段长度截取 0:不做截取操作',
  `override` tinyint(4) DEFAULT NULL COMMENT '是否覆盖当前脱敏配置(0:不覆盖,1:覆盖)',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_dbus_datasource
-- ----------------------------
DROP TABLE IF EXISTS `t_dbus_datasource`;
CREATE TABLE `t_dbus_datasource` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ds_name` varchar(64) NOT NULL DEFAULT '' COMMENT '数据源名字',
  `ds_type` varchar(32) NOT NULL DEFAULT '' COMMENT '数据源类型oracle/mysql',
  `instance_name` varchar(32) DEFAULT NULL,
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
  `ds_partition` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uidx_ds_name` (`ds_name`),
  UNIQUE KEY `ds_name` (`ds_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='dbus数据源配置表';

-- ----------------------------
-- Table structure for t_ddl_event
-- ----------------------------
DROP TABLE IF EXISTS `t_ddl_event`;
CREATE TABLE `t_ddl_event` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `event_id` int(11) unsigned DEFAULT NULL COMMENT 'oracle源端event表的serno',
  `ds_id` int(11) NOT NULL,
  `schema_name` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `column_name` varchar(64) DEFAULT NULL,
  `ver_id` int(11) unsigned NOT NULL COMMENT '当前表版本id',
  `trigger_ver` int(11) unsigned DEFAULT NULL COMMENT 'oracle源端event表中version',
  `ddl_type` varchar(64) NOT NULL,
  `ddl` varchar(3000) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_encode_plugins
-- ----------------------------
DROP TABLE IF EXISTS `t_encode_plugins`;
CREATE TABLE `t_encode_plugins` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) DEFAULT NULL COMMENT '名称',
  `project_id` int(11) DEFAULT NULL COMMENT '项目ID',
  `path` varchar(256) DEFAULT NULL COMMENT '存储路径',
  `encoders` varchar(1024) DEFAULT NULL,
  `status` varchar(32) DEFAULT NULL COMMENT '状态',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_fullpull_history
-- ----------------------------
DROP TABLE IF EXISTS `t_fullpull_history`;
CREATE TABLE `t_fullpull_history` (
  `id` bigint(20) NOT NULL,
  `type` varchar(20) NOT NULL COMMENT 'global, indepent, normal',
  `dsName` varchar(64) NOT NULL,
  `schemaName` varchar(64) NOT NULL,
  `tableName` varchar(64) NOT NULL,
  `version` int(11) DEFAULT NULL,
  `batch_id` int(11) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL COMMENT 'init, splitting, pulling, ending, abort',
  `error_msg` varchar(8096) DEFAULT NULL,
  `init_time` datetime DEFAULT NULL,
  `start_split_time` datetime DEFAULT NULL,
  `start_pull_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `finished_partition_count` bigint(20) DEFAULT NULL,
  `total_partition_count` bigint(20) DEFAULT NULL,
  `finished_row_count` bigint(20) DEFAULT NULL,
  `total_row_count` bigint(20) DEFAULT NULL,
  `project_name` varchar(128) DEFAULT NULL,
  `topology_table_id` int(11) DEFAULT NULL,
  `target_sink_id` int(11) DEFAULT NULL,
  `target_sink_topic` varchar(256) DEFAULT NULL,
  `full_pull_req_msg_offset` bigint(20) DEFAULT NULL,
  `first_shard_msg_offset` bigint(20) DEFAULT NULL,
  `last_shard_msg_offset` bigint(20) DEFAULT NULL,
  `split_column` varchar(64) DEFAULT NULL,
  `fullpull_condition` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

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
  `comments` varchar(128) DEFAULT NULL COMMENT '表注释',
  PRIMARY KEY (`id`),
  KEY `idx_event_offset` (`event_offset`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='t_table_meta的版本信息';

-- ----------------------------
-- Table structure for t_plain_log_rule_group
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_group`;
CREATE TABLE `t_plain_log_rule_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) NOT NULL,
  `group_name` varchar(64) NOT NULL,
  `status` varchar(16) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `json_extract_rule_xml_pack` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_plain_log_rule_group_version
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_group_version`;
CREATE TABLE `t_plain_log_rule_group_version` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) NOT NULL,
  `group_name` varchar(64) NOT NULL,
  `status` varchar(16) NOT NULL,
  `ver_id` int(11) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_plain_log_rule_type
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rule_type`;
CREATE TABLE `t_plain_log_rule_type` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `rule_type_name` varchar(16) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_plain_log_rules
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rules`;
CREATE TABLE `t_plain_log_rules` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `order_id` int(4) NOT NULL,
  `rule_type_name` varchar(16) NOT NULL,
  `rule_grammar` mediumtext NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_plain_log_rules_version
-- ----------------------------
DROP TABLE IF EXISTS `t_plain_log_rules_version`;
CREATE TABLE `t_plain_log_rules_version` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` int(11) NOT NULL,
  `order_id` int(4) NOT NULL,
  `rule_type_name` varchar(16) NOT NULL,
  `rule_grammar` varchar(20480) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project
-- ----------------------------
DROP TABLE IF EXISTS `t_project`;
CREATE TABLE `t_project` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `project_name` varchar(128) DEFAULT NULL COMMENT '项目名称',
  `project_display_name` varchar(32) NOT NULL DEFAULT '' COMMENT '可以用中文。长度不超过32个中文字。 要求唯一。仅作显示用',
  `project_owner` varchar(32) DEFAULT NULL,
  `project_icon` varchar(128) DEFAULT NULL,
  `project_desc` varchar(256) DEFAULT NULL COMMENT '项目描述',
  `project_expire` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '项目有效期',
  `topology_num` tinyint(4) DEFAULT NULL,
  `allow_admin_manage` tinyint(4) DEFAULT NULL,
  `schema_change_notify_flag` tinyint(4) DEFAULT '1' COMMENT '备同步报警通知开关',
  `schema_change_notify_emails` varchar(1024) DEFAULT NULL,
  `slave_sync_delay_notify_flag` tinyint(4) DEFAULT '1' COMMENT '主备同步报警通知开关',
  `slave_sync_delay_notify_emails` varchar(1024) DEFAULT NULL,
  `fullpull_notify_flag` tinyint(4) DEFAULT '1' COMMENT '拉全量报警通知',
  `fullpull_notify_emails` varchar(1024) DEFAULT NULL,
  `data_delay_notify_flag` tinyint(4) DEFAULT NULL,
  `data_delay_notify_emails` varchar(1024) DEFAULT NULL,
  `status` varchar(16) DEFAULT 'active' COMMENT '项目状态 active，inactive',
  `storm_start_path` varchar(1024) DEFAULT NULL COMMENT '启动storm start地址',
  `storm_ssh_user` varchar(64) DEFAULT NULL COMMENT '启动storm nimbus jar包用户',
  `storm_api_url` varchar(1024) DEFAULT NULL COMMENT '调用storm api地址',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `keytab_path` varchar(256) DEFAULT NULL COMMENT 'Kerberos秘钥存储path',
  `principal` varchar(256) DEFAULT NULL COMMENT 'Kerberos账户',
  PRIMARY KEY (`id`),
  UNIQUE KEY `project_display_name` (`project_display_name`) USING BTREE,
  UNIQUE KEY `project_name` (`project_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='剩余时间，给前端判断颜色，同时标识是否发送了通知邮件';

-- ----------------------------
-- Table structure for t_project_encode_hint
-- ----------------------------
DROP TABLE IF EXISTS `t_project_encode_hint`;
CREATE TABLE `t_project_encode_hint` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) DEFAULT NULL,
  `table_id` int(11) DEFAULT NULL,
  `field_name` varchar(64) DEFAULT NULL COMMENT '字段名称',
  `encode_plugin_id` int(11) DEFAULT NULL COMMENT '脱敏插件ID',
  `encode_type` varchar(64) DEFAULT NULL COMMENT '脱敏方式',
  `encode_param` varchar(4096) DEFAULT NULL COMMENT '脱敏使用的参数',
  `desc_` varchar(64) DEFAULT NULL COMMENT '描述',
  `truncate` int(1) DEFAULT '0' COMMENT '1:当字符串类型字段值脱敏后超出源表字段长度时按照源表字段长度截取 0:不做截取操作',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_resource
-- ----------------------------
DROP TABLE IF EXISTS `t_project_resource`;
CREATE TABLE `t_project_resource` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `project_id` int(11) unsigned NOT NULL COMMENT 'project id',
  `table_id` int(11) unsigned NOT NULL COMMENT 'dbus mgr中的table id',
  `fullpull_enable_flag` tinyint(3) unsigned NOT NULL DEFAULT '1',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_projectid_tableid` (`project_id`,`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_sink
-- ----------------------------
DROP TABLE IF EXISTS `t_project_sink`;
CREATE TABLE `t_project_sink` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL,
  `sink_id` varchar(64) NOT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_topo
-- ----------------------------
DROP TABLE IF EXISTS `t_project_topo`;
CREATE TABLE `t_project_topo` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `topo_name` varchar(64) NOT NULL COMMENT 'topo name',
  `topo_config` varchar(1024) DEFAULT NULL,
  `jar_version` varchar(32) DEFAULT NULL,
  `jar_file_path` varchar(256) DEFAULT NULL,
  `status` varchar(32) DEFAULT NULL COMMENT 'stopped,running,changed(修改了表的配置,比如脱敏)',
  `topo_comment` varchar(256) DEFAULT NULL,
  `project_id` int(11) unsigned DEFAULT NULL COMMENT 'project id',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_status_toponame` (`status`,`topo_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_topo_table
-- ----------------------------
DROP TABLE IF EXISTS `t_project_topo_table`;
CREATE TABLE `t_project_topo_table` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `project_id` int(11) unsigned NOT NULL COMMENT 'project table id',
  `table_id` int(11) NOT NULL,
  `topo_id` int(11) unsigned NOT NULL COMMENT 'project id',
  `status` varchar(32) NOT NULL COMMENT 'new,changed,stopped, running',
  `output_topic` varchar(256) NOT NULL COMMENT 'output topic',
  `output_type` varchar(32) DEFAULT NULL COMMENT '输出数据格式: json/ums_1.*',
  `sink_id` int(11) DEFAULT NULL,
  `output_list_type` int(1) DEFAULT NULL COMMENT '输出列的列类型：0，贴源输出表的所有列（输出列随源端schema变动而变动；1，指定固定的输出列（任何时候只输出您此时此地选定的列）',
  `meta_ver` int(11) DEFAULT NULL COMMENT '为指定输出列时才有meta version',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `schema_change_flag` tinyint(4) DEFAULT '0' COMMENT '表结构变更标志0:未变更,1:变更',
  `fullpull_col` varchar(255) DEFAULT NULL COMMENT '全量分片列:配置column名称',
  `fullpull_split_shard_size` varchar(255) DEFAULT NULL COMMENT '全量分片大小配置:配置-1代表不分片',
  `fullpull_split_style` varchar(255) DEFAULT NULL COMMENT '全量分片类型:MD5',
  `fullpull_condition` varchar(500) DEFAULT NULL COMMENT '全量拉取条件,例如id>100',
  PRIMARY KEY (`id`),
  KEY `idx_projectid_tableid_topoid` (`project_id`,`table_id`,`topo_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_topo_table_encode_output_columns
-- ----------------------------
DROP TABLE IF EXISTS `t_project_topo_table_encode_output_columns`;
CREATE TABLE `t_project_topo_table_encode_output_columns` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_topo_table_id` int(11) DEFAULT NULL,
  `field_name` varchar(64) DEFAULT NULL COMMENT '字段名称',
  `encode_plugin_id` int(11) DEFAULT NULL COMMENT '脱敏插件ID',
  `field_type` varchar(64) DEFAULT NULL COMMENT '字段类型',
  `data_length` bigint(20) DEFAULT NULL COMMENT '数据长度',
  `encode_type` varchar(64) DEFAULT NULL COMMENT '脱敏方式',
  `encode_param` varchar(4096) DEFAULT NULL COMMENT '脱敏使用的参数',
  `desc_` varchar(64) DEFAULT NULL COMMENT '描述',
  `truncate` int(1) DEFAULT '0' COMMENT '1:当字符串类型字段值脱敏后超出源表字段长度时按照源表字段长度截取 0:不做截取操作',
  `encode_source` int(1) DEFAULT NULL COMMENT '脱敏规则的来源：0，源端脱敏，最开始的dtables包含的脱敏信息；1，admin脱敏，新增project操作中，新增Resource中配置的脱敏信息；2，自定义脱敏，除前两种列外，用户添加的列的脱敏信息；3，无，表示没有脱敏信息。',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `schema_change_flag` tinyint(4) DEFAULT '0' COMMENT '表结构变更标志0:未变更,1:变更',
  `data_scale` int(11) DEFAULT '0' COMMENT '小数部分长度',
  `data_precision` int(11) DEFAULT '0' COMMENT '数据精度',
  `schema_change_comment` varchar(1024) DEFAULT '' COMMENT '记录变更过程',
  `special_approve` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否经过特批(0:未特批,1:特批(特批过的不用脱敏))',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='存储topo表中输出列的信息，如果某些列配置了脱敏信息，则会显示脱敏信息；';

-- ----------------------------
-- Table structure for t_project_topo_table_meta_version
-- ----------------------------
DROP TABLE IF EXISTS `t_project_topo_table_meta_version`;
CREATE TABLE `t_project_topo_table_meta_version` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL,
  `topo_id` int(11) NOT NULL,
  `table_id` int(11) NOT NULL COMMENT '源table表的id，非t_project_topo_table的id',
  `version` int(11) DEFAULT NULL,
  `column_name` varchar(64) DEFAULT NULL,
  `data_type` varchar(64) DEFAULT NULL,
  `data_length` bigint(20) DEFAULT NULL COMMENT '数据长度',
  `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `schema_change_flag` tinyint(4) DEFAULT '0' COMMENT '表结构变更标志0:未变更,1:变更',
  `data_precision` int(11) DEFAULT '0' COMMENT '数据精度',
  `data_scale` int(11) DEFAULT '0' COMMENT '小数部分长度',
  `schema_change_comment` varchar(1024) DEFAULT '' COMMENT '记录变更过程',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_project_user
-- ----------------------------
DROP TABLE IF EXISTS `t_project_user`;
CREATE TABLE `t_project_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `project_id` int(11) unsigned NOT NULL,
  `user_id` int(11) unsigned NOT NULL,
  `user_level` varchar(32) NOT NULL DEFAULT 'full' COMMENT '用户在项目中的权限：full 或者 readonly',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_query_rule_group
-- ----------------------------
DROP TABLE IF EXISTS `t_query_rule_group`;
CREATE TABLE `t_query_rule_group` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '表的主键',
  `table_id` bigint(20) DEFAULT NULL COMMENT 't_dbus_datasource 表的 id',
  `ver_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL COMMENT '状态 active 有效 inactive 无效',
  `description` varchar(256) DEFAULT NULL,
  `query` varchar(4000) DEFAULT NULL COMMENT '获取数据的 sql 或者其他查询语句',
  `time_field` varchar(256) DEFAULT NULL COMMENT '生成batch的字段',
  `init_time` datetime DEFAULT NULL COMMENT '生成 batch 的初始时间',
  `time_span_ms` bigint(20) DEFAULT NULL COMMENT '生成 batch 的时间间隔',
  `correction_ms` bigint(20) DEFAULT NULL COMMENT '开始时间修正值',
  `reserve_time_ms` bigint(20) DEFAULT NULL COMMENT '保留时间毫秒数(系统当前时间 - end_time < reserve_time_ms  的数据不能获取)',
  `output_style` varchar(255) DEFAULT NULL COMMENT '输出json的格式，flatten：一级扁平展开输出，default：一级展开输出',
  `save_as_cols` varchar(10240) DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_query_rule_ver_id` (`ver_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

-- ----------------------------
-- Table structure for t_sink
-- ----------------------------
DROP TABLE IF EXISTS `t_sink`;
CREATE TABLE `t_sink` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sink_name` varchar(64) DEFAULT NULL,
  `sink_desc` varchar(255) DEFAULT NULL,
  `sink_type` varchar(32) DEFAULT NULL,
  `url` varchar(512) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `is_global` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_table_action
-- ----------------------------
DROP TABLE IF EXISTS `t_table_action`;
CREATE TABLE `t_table_action` (
  `id` bigint(20) NOT NULL,
  `table_id` bigint(20) DEFAULT NULL,
  `order_id` int(11) DEFAULT NULL,
  `action_source` varchar(50) DEFAULT NULL,
  `action_type` varchar(60) DEFAULT NULL,
  `action_value` varchar(200) DEFAULT NULL,
  `commit` varchar(100) DEFAULT NULL,
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
  `data_length` bigint(20) NOT NULL COMMENT '数据长度',
  `data_precision` int(11) DEFAULT NULL COMMENT '数据精度',
  `data_scale` int(11) DEFAULT NULL COMMENT '小数部分长度',
  `char_length` int(11) DEFAULT NULL,
  `char_used` varchar(1) DEFAULT NULL,
  `nullable` varchar(1) NOT NULL DEFAULT '' COMMENT '是否可空Y/N',
  `is_pk` varchar(1) NOT NULL DEFAULT '' COMMENT '是否为主键Y/N',
  `pk_position` int(2) DEFAULT NULL COMMENT '主键的顺序',
  `alter_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建/修改的时间',
  `comments` varchar(512) DEFAULT NULL COMMENT '列注释',
  `default_value` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_table_meta` (`ver_id`,`column_name`,`original_ser`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='抓取的源表数据meta信息';

-- ----------------------------
-- Table structure for t_user
-- ----------------------------
DROP TABLE IF EXISTS `t_user`;
CREATE TABLE `t_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `role_type` varchar(16) NOT NULL DEFAULT 'user' COMMENT '用户类型',
  `status` varchar(16) NOT NULL DEFAULT 'active' COMMENT '用户状态',
  `user_name` varchar(128) NOT NULL COMMENT '用户名',
  `password` varchar(256) DEFAULT NULL COMMENT '密码',
  `email` varchar(128) NOT NULL DEFAULT '' COMMENT 'email',
  `phone_num` varchar(32) DEFAULT '' COMMENT '电话号码',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_user_email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
