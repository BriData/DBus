/**
 * @author 戎晓伟
 * @description API
 */

// JAR包搜索
export const SEARCH_JAR_INFOS_API = '/keeper/jars/infos'
// JAR包批量删除
export const BATCH_DELETE_JAR_API = '/keeper/jars/batch-delete'
// JAR包上传
export const UPLOAD_JAR_API = '/keeper/jars/uploads'
// JAR包获取Version列表
export const JAR_GET_VERSION_LIST_API = '/keeper/jars/versions'
// JAR包获取Type列表
export const JAR_GET_TYPE_LIST_API = '/keeper/jars/types'


// DataSource首页的搜索
export const DATA_SOURCE_SEARCH_API = '/keeper/data-source/search'
// DataSource名称和id的列表，提供给schema级应用使用
export const DATA_SOURCE_GET_ID_TYPE_NAME_API = '/keeper/data-source/getDSNames'
// DataSource通过id获取
export const DATA_SOURCE_GET_BY_ID_API = '/keeper/data-source'
// DataSource删除
export const DATA_SOURCE_DELETE_API = '/keeper/data-source/delete'
// DataSource修改
export const DATA_SOURCE_UPDATE_API = '/keeper/data-source/update'
// DataSource增加
export const DATA_SOURCE_INSERT_API = '/keeper/data-source/insert'
// topology kill
export const KILL_TOPOLOGY_API = '/keeper/data-source/kill'
// 获取schema list by dsId
export const DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID_API = '/keeper/data-schema/source-schemas'
// 获取schema table
export const DATA_SOURCE_GET_SCHEMA_TABLE_LIST_API = '/keeper/data-schema/schema-tables'
// 清除全量报警
export const DATA_SOURCE_CLEAR_FULLPULL_ALARM_API = '/keeper/fullpullHistory/clearFullPullAlarm'
// 添加schema table
export const DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API = '/keeper/data-schema/schema-tables'
// 克隆ZK模板
export const CLONE_CONF_FROM_TEMPLATE_API = '/keeper/zookeeper/cloneConfFromTemplate'
// 获取最新的jar包路径
export const LATEST_JAR_GET_PATH_API = '/keeper/data-source/paths'
// 启动topo
export const TOPO_JAR_START_API = '/keeper/data-source/startTopology'
// 查看日志
export const VIEW_LOG_API = '/keeper/data-source/view-log'
// 批量加表
export const DATA_SOURCE_BATCH_ADD_TABLE_API = '/keeper/data-service/batchAddTables/addSchemaAndTables'
// 批量检查
export const DATA_SOURCE_PRE_PROCESS_API = '/keeper/data-service/accessDbusPreTreated/check'
// 生成OGG Trail前缀
export const DATA_SOURCE_GENERATE_OGG_TRAIN_NAME_API = '/keeper/autoDeploy/getOggTrailName'

// 添加data source
export const DATA_SOURCE_ADD_API = '/keeper/data-source'
// 检查数据源连通性
export const DATA_SOURCE_VALIDATE_API = '/keeper/data-source/validate'
// 数据源拖回重跑
export const DATA_SOURCE_RERUN_API = '/keeper/data-source/rerun'
// 获取OGG配置
export const DATA_SOURCE_GET_OGG_CONF_API = '/keeper/autoDeploy/getOggConf'
// 设置OGG配置
export const DATA_SOURCE_SET_OGG_CONF_API = '/keeper/autoDeploy/setOggConf'
// 获取canal配置
export const DATA_SOURCE_GET_CANAL_CONF_API = '/keeper/autoDeploy/getCanalConf'
// 设置canal配置
export const DATA_SOURCE_SET_CANAL_CONF_API = '/keeper/autoDeploy/setCanalConf'


// DataSchema首页的搜索
export const DATA_SCHEMA_SEARCH_API = '/keeper/data-schema/search'
// DataSchema搜索所有信息，不需要分页
export const DATA_SCHEMA_SEARCH_ALL_LIST_API = '/keeper/data-schema/searchAll'
// DataSchema通过id获取
export const DATA_SCHEMA_GET_BY_ID_API = '/keeper/data-schema'
// DataSchema删除
export const DATA_SCHEMA_DELETE_API = '/keeper/data-schema/delete'
// DataSchema修改
export const DATA_SCHEMA_UPDATE_API = '/keeper/data-schema/update'
// DataSchema增加
export const DATA_SCHEMA_INSERT_API = '/keeper/data-schema/insert'
// DataSchema拖回重跑
export const DATA_SCHEMA_RERUN_API = '/keeper/data-schema/rerun'

// DataTable首页的搜索
export const DATA_TABLE_SEARCH_API = '/keeper/tables/find'
// DataTable搜索所有，不分页
export const DATA_TABLE_FIND_ALL_SEARCH_API = '/keeper/tables/findAll'
// DataTable删除
export const DATA_TABLE_DELETE_API = '/keeper/tables/delete'
// DataTable修改
export const DATA_TABLE_UPDATE_API = '/keeper/tables/updateTable'
// DataTable增加
export const DATA_TABLE_INSERT_API = '/keeper/tables/insert'
// DataTable Start
export const DATA_TABLE_START_API = '/keeper/tables/activate'
// DataTable批量启动
export const DATA_TABLE_BATCH_START_API = '/keeper/tables/batchStartTableByTableIds'
// DataTable批量停止
export const DATA_TABLE_BATCH_STOP_API = '/keeper/tables/batchStopTableByTableIds'
// DataTable Stop
export const DATA_TABLE_STOP_API = '/keeper/tables/deactivate'
// DataTable 拖回重跑
export const DATA_TABLE_RERUN_API = '/keeper/tables/rerun'
// DBusData 查询源端表和存储过程
export const DBUS_DATA_SEARCH_FROM_SOURCE_API = '/keeper/data-source/searchFromSource'
// DBusData 执行SQL
export const DBUS_DATA_EXECUTE_SQL_API = '/keeper/tables/executeSql'

// Encode manager
export const ENCODE_MANAGER_SEARCH_API = '/keeper/encode/search'

// 查询脱敏配置
export const DATA_TABLE_GET_ENCODE_CONFIG_API = '/keeper/tables/desensitization'
// 查询脱敏表meta
export const DATA_TABLE_GET_TABLE_COLUMN_API = '/keeper/tables/fetchTableColumns'
// 查询脱敏类型
export const DATA_TABLE_GET_ENCODE_TYPE_API = '/keeper/tables/fetchEncodeAlgorithms'
// 保存脱敏配置
export const DATA_TABLE_SAVE_ENCODE_CONFIG_API = '/keeper/tables/changeDesensitization'
// 保存脱敏配置
export const DATA_TABLE_CHECK_DATA_LINE_API = '/keeper/flow-line-check/check'
// 查看源表数据
export const DATA_TABLE_SOURCE_INSIGHT_API = '/keeper/toolSet/sourceTableColumn'

// 查询表版本历史
export const DATA_TABLE_GET_VERSION_LIST_API = '/keeper/tables/getVersionListByTableId'
// 查询表版本详细信息
export const DATA_TABLE_GET_VERSION_DETAIL_API = '/keeper/tables/getVersionDetail'

// 规则组 查询
export const SEARCH_RULE_GROUP_API = '/keeper/tables/getAllRuleGroup'
// 规则组 新增
export const ADD_RULE_GROUP_API = '/keeper/tables/addGroup'
// 规则组 克隆
export const CLONE_RULE_GROUP_API = '/keeper/tables/cloneRuleGroup'
// 规则组 删除
export const DELETE_RULE_GROUP_API = '/keeper/tables/deleteRuleGroup'
// 规则组 更新
export const UPDATE_RULE_GROUP_API = '/keeper/tables/updateRuleGroup'
// 规则组 升级
export const UPGRADE_RULE_GROUP_API = '/keeper/tables/upgradeVersion'
// 规则组 对比
export const DIFF_RULE_GROUP_API = '/keeper/tables/diffGroupRule'


// 规则 查询
export const GET_ALL_RULES_API = '/keeper/tables/getAllRules'
// 规则 执行
export const EXECUTE_RULES_API = '/keeper/tables/executeRules'
// 规则 保存
export const SAVE_ALL_RULES_API = '/keeper/tables/saveAllRules'

// 导入规则
export const IMPORT_RULES_API = '/keeper/tables/importRulesByTableId'
// 导出规则
export const EXPORT_RULES_API = '/keeper/tables/exportRulesByTableId'

// 脱敏插件查询
export const SEARCH_ENCODE_PLUGIN_API = '/keeper/jars/search-encode-plugin'
// 脱敏插件删除
export const DELETE_ENCODE_PLUGIN_API = '/keeper/jars/delete-encode-plugin'
// 脱敏插件上传
export const UPLOAD_ENCODE_PLUGIN_API = '/keeper/jars/uploads-encode-plugin'

// 上传key
export const UPLOAD_USER_KEY_API = '/keeper/jars/uploads-keytab'
// 查询key列表
export const SEARCH_USER_KEY_API = '/keeper/projects/search'
// 下载key
export const DOWNLOAD_USER_KEY_API = '/keeper/jars/download-keytab'
