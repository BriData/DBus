ALTER TABLE `t_data_tables`
ADD COLUMN `fullpull_col`  varchar(255) NULL COMMENT '全量分片列:配置column名称' AFTER `description`,
ADD COLUMN `fullpull_split_shard_size`  varchar(255) NULL COMMENT '全量分片大小配置:配置-1代表不分片' AFTER `fullpull_col`,
ADD COLUMN `fullpull_split_style`  varchar(255) NULL COMMENT '全量分片类型:MD5' AFTER `fullpull_split_shard_size`,
ADD COLUMN `is_open`  int(1) NULL DEFAULT 0 COMMENT 'mongo是否展开节点,0不展开,1一级展开' AFTER `fullpull_split_style`,
ADD COLUMN `is_auto_complete`  tinyint(4) NULL DEFAULT 0 COMMENT 'mongoDB的表是否补全数据；如果开启，增量中更新操作会回查并补全数据' AFTER `is_open`;


ALTER TABLE `t_dbus_datasource`
ADD COLUMN `instance_name`  varchar(32) NULL COMMENT '数据库示例名称' AFTER `ds_type`;

ALTER TABLE `t_fullpull_history`
ADD COLUMN `split_column`  varchar(64) NULL COMMENT '分片列' AFTER `last_shard_msg_offset`;
ADD COLUMN `fullpull_condition`  varchar(512) NULL COMMENT '拉全量条件' AFTER `split_column`;


ALTER TABLE `t_project_topo_table`
ADD COLUMN `fullpull_col`  varchar(255) NULL COMMENT '全量分片列:配置column名称' AFTER `schema_change_flag`;
ADD COLUMN `fullpull_split_shard_size`  varchar(255) NULL COMMENT '全量分片大小配置:配置-1代表不分片' AFTER `fullpull_col`;
ADD COLUMN `fullpull_split_style`  varchar(255) NULL COMMENT '全量分片类型:MD5' AFTER `fullpull_split_shard_size`;
ADD COLUMN `fullpull_condition`  varchar(255) NULL COMMENT '全量拉取条件,例如id>100' AFTER `fullpull_split_style`;



ALTER TABLE `t_project_topo_table_encode_output_columns`
ADD COLUMN `special_approve`  tinyint(4) NULL DEFAULT '0' COMMENT '是否经过特批(0:未特批,1:特批(特批过的不用脱敏))' AFTER `schema_change_comment`;


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