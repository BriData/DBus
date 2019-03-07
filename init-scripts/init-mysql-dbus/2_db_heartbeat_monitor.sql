SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for db_heartbeat_monitor
-- ----------------------------
DROP TABLE IF EXISTS `db_heartbeat_monitor`;
CREATE TABLE `db_heartbeat_monitor` (
  `ID` bigint(19) NOT NULL AUTO_INCREMENT COMMENT '心跳表主键',
  `DS_NAME` varchar(64) NOT NULL COMMENT '数据源名称',
  `SCHEMA_NAME` varchar(64) NOT NULL COMMENT '用户名',
  `TABLE_NAME` varchar(64) NOT NULL COMMENT '表名',
  `PACKET` varchar(256) NOT NULL COMMENT '数据包',
  `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` datetime NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
