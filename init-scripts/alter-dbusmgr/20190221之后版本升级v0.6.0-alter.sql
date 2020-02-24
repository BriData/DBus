RENAME TABLE `t_dba_encode_columns` TO `t_encode_columns`;

ALTER TABLE `t_encode_columns`
DROP  COLUMN `override`;

ALTER TABLE `t_data_schema`
ADD COLUMN  `db_vip` varchar(32) DEFAULT NULL COMMENT 'schema对应数据库虚IP';

ALTER TABLE `t_data_tables`
ADD COLUMN  `fullpull_condition` varchar(512) DEFAULT NULL COMMENT '全量条件';

ALTER TABLE `t_fullpull_history`
ADD COLUMN  `current_shard_offset` bigint(20) DEFAULT NULL;

ALTER TABLE `t_meta_version`
ADD COLUMN  `schema_hash` int(11) DEFAULT NULL COMMENT '该版本数据对应的schema哈希值',
ADD COLUMN  `abandon` TINYINT(4) DEFAULT NULL COMMENT '假删除标记,0或null表示未删除,1表示删除';

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

CREATE TABLE `t_storm_topology` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topology_name` varchar(128) NOT NULL,
  `ds_id` int(11) NOT NULL,
  `jar_name` varchar(512) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
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