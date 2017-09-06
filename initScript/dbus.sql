create database dbus;

-- dbus密码可由dba指定
CREATE USER dbus IDENTIFIED BY '你的密码';

GRANT ALL ON dbus.* TO dbus@'%' IDENTIFIED BY '你的密码';

-- schemaName1.tableName1 是需要同步的表名
GRANT select on schemaName1.tableName1   TO dbus; 

flush privileges;

USE `dbus`;

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for db_heartbeat_monitor
-- ----------------------------
DROP TABLE IF EXISTS `db_heartbeat_monitor`;
CREATE TABLE `db_heartbeat_monitor` (
  `ID` bigint(19) NOT NULL AUTO_INCREMENT COMMENT '心跳表主键',
  `DS_NAME` varchar(32) NOT NULL COMMENT '数据源名称',
  `SCHEMA_NAME` varchar(32) NOT NULL COMMENT '用户名',
  `TABLE_NAME` varchar(64) NOT NULL COMMENT '表名',
  `PACKET` varchar(256) NOT NULL COMMENT '数据包',
  `CREATE_TIME` datetime(3) NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` datetime(3) NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=5305484 DEFAULT CHARSET=utf8;


SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for db_full_pull_requests
-- ----------------------------
DROP TABLE IF EXISTS `db_full_pull_requests`;
CREATE TABLE `db_full_pull_requests` (
  `seqno` int(11) NOT NULL AUTO_INCREMENT,
  `schema_name` varchar(32) DEFAULT NULL,
  `table_name` varchar(50) NOT NULL,
  `physical_tables` varchar(10240) DEFAULT NULL,
  `scn_no` int(11) DEFAULT NULL,
  `split_col` varchar(50) DEFAULT NULL,
  `split_bounding_query` varchar(512) DEFAULT NULL,
  `pull_target_cols` varchar(512) DEFAULT NULL,
  `pull_req_create_time` timestamp(6) NOT NULL,
  `pull_start_time` timestamp(6) NULL DEFAULT NULL,
  `pull_end_time` timestamp(6) NULL DEFAULT NULL,
  `pull_status` varchar(16) DEFAULT NULL,
  `pull_remark` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`seqno`)
) ENGINE=InnoDB AUTO_INCREMENT=762 DEFAULT CHARSET=utf8;





