SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for db_full_pull_requests
-- ----------------------------
DROP TABLE IF EXISTS `db_full_pull_requests`;
CREATE TABLE `db_full_pull_requests` (
  `seqno` bigint(19) NOT NULL AUTO_INCREMENT,
  `schema_name` varchar(64) DEFAULT NULL,
  `table_name` varchar(64) NOT NULL,
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
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
