ALTER TABLE `t_data_schema`
ADD COLUMN  `db_vip` varchar(32) DEFAULT NULL COMMENT 'schema对应数据库虚IP';

ALTER TABLE `t_data_tables`
MODIFY COLUMN  `status` varchar(32) NOT NULL DEFAULT 'abort' COMMENT 'ok,abort,inactive,waiting\r\nok:正常使用;abort:需要抛弃该表的数据;waiting:等待拉全量;inactive:不可用 ',
MODIFY COLUMN  `fullpull_col` varchar(255) DEFAULT '' COMMENT '全量分片列:配置column名称',
MODIFY COLUMN  `fullpull_split_shard_size` varchar(255) DEFAULT '' COMMENT '全量分片大小配置:配置-1代表不分片',
MODIFY COLUMN  `fullpull_split_style` varchar(255) DEFAULT '' COMMENT '全量分片类型:MD5',
ADD COLUMN  `fullpull_condition` varchar(512) DEFAULT NULL COMMENT '全量条件',
ADD COLUMN  `is_open` int(1) DEFAULT '0' COMMENT 'mongo是否展开节点,0不展开,1一级展开',
ADD COLUMN  `is_auto_complete` tinyint(4) DEFAULT '0' COMMENT 'mongoDB的表是否补全数据；如果开启，增量中更新操作会回查并补全数据';

ALTER TABLE `t_dbus_datasource`
ADD COLUMN  `instance_name` varchar(32) DEFAULT NULL;

ALTER TABLE `t_encode_columns`
MODIFY COLUMN  `plugin_id` int(11) DEFAULT NULL COMMENT '脱敏插件ID';

ALTER TABLE `t_fullpull_history`
ADD COLUMN  `split_column` varchar(64) DEFAULT NULL,
ADD COLUMN  `fullpull_condition` varchar(512) DEFAULT NULL,
ADD COLUMN  `current_shard_offset` bigint(20) DEFAULT NULL,
DROP COLUMN  `cost_time`,
DROP COLUMN  `complete_rate`;

ALTER TABLE `t_meta_version`
ADD COLUMN  `schema_hash` int(11) DEFAULT NULL COMMENT '该版本数据对应的schema哈希值',
ADD COLUMN  `abandon` TINYINT(4) DEFAULT NULL COMMENT '假删除标记,0或null表示未删除,1表示删除';

ALTER TABLE `t_project_resource`
ADD  INDEX `idx_projectid_tableid` (`project_id`,`table_id`);

ALTER TABLE `t_project_topo_table_encode_output_columns`
MODIFY COLUMN  `data_length` bigint(20) DEFAULT NULL COMMENT '数据长度',
ADD COLUMN  `special_approve` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否经过特批(0:未特批,1:特批(特批过的不用脱敏))';

ALTER TABLE `t_project_topo_table_meta_version`
MODIFY COLUMN  `data_length` bigint(20) DEFAULT NULL COMMENT '数据长度';

ALTER TABLE `t_project_topo_table`
MODIFY COLUMN  `status` varchar(32) NOT NULL COMMENT 'new,changed,stopped, running',
ADD COLUMN  `fullpull_col` varchar(255) DEFAULT NULL COMMENT '全量分片列:配置column名称',
ADD COLUMN  `fullpull_split_shard_size` varchar(255) DEFAULT NULL COMMENT '全量分片大小配置:配置-1代表不分片',
ADD COLUMN  `fullpull_split_style` varchar(255) DEFAULT NULL COMMENT '全量分片类型:MD5',
ADD COLUMN  `fullpull_condition` varchar(500) DEFAULT NULL COMMENT '全量拉取条件,例如id>100',
ADD  INDEX `idx_projectid_tableid_topoid` (`project_id`,`table_id`,`topo_id`);

ALTER TABLE `t_project_topo`
MODIFY COLUMN  `status` varchar(32) DEFAULT NULL COMMENT 'stopped,running,changed(修改了表的配置,比如脱敏)',
ADD  INDEX `idx_status_toponame` (`status`,`topo_name`);

ALTER TABLE `t_sink`
ADD  UNIQUE INDEX `idx_sinker_name_unique` (`sink_name`);

ALTER TABLE `t_table_meta`
MODIFY COLUMN  `comments` varchar(1024) DEFAULT NULL COMMENT '列注释';

CREATE TABLE `t_name_alias_mapping` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `type` int(2) NOT NULL COMMENT '别名类型1,router拓扑别名;2,增量拓扑别名',
  `name` varchar(64) NOT NULL COMMENT '名称',
  `name_id` int(11) NOT NULL COMMENT '名称对应ID',
  `alias` varchar(64) NOT NULL COMMENT '别名',
  `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

CREATE TABLE `t_sinker_topo_schema` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sinker_topo_id` int(11) DEFAULT NULL,
  `sinker_name` varchar(64) DEFAULT NULL,
  `ds_id` int(11) DEFAULT NULL,
  `ds_name` varchar(32) DEFAULT NULL,
  `schema_id` int(11) DEFAULT NULL,
  `schema_name` varchar(32) DEFAULT NULL,
  `target_topic` varchar(64) DEFAULT NULL,
  `description` varchar(128) DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_schema_id_unique` (`schema_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

CREATE TABLE `t_sinker_topo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sinker_name` varchar(64) DEFAULT NULL,
  `sinker_conf` varchar(1024) DEFAULT NULL,
  `jar_id` int(11) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `description` varchar(128) DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

CREATE TABLE `t_topology_jar` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `category` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `path` varchar(255) DEFAULT NULL,
  `minor_version` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;