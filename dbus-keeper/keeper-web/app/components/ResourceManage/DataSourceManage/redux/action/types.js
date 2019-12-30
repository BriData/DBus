/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// DataSource管理查询参数
export const DATA_SOURCE_ALL_PARAMS = 'dataSource/dataSourceManager/DATA_SOURCE_ALL_PARAMS'

// DataSource管理查询
export const DATA_SOURCE_ALL_SEARCH = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource名称和id的列表，提供给schema级应用使用
export const DATA_SOURCE_GET_ID_TYPE_NAME = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_GET_ID_TYPE_NAME', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource管理查询
export const DATA_SOURCE_GET_BY_ID = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_GET_BY_ID', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource删除
export const DATA_SOURCE_DELETE = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_DELETE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 修改
export const DATA_SOURCE_UPDATE = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_UPDATE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 增加
export const DATA_SOURCE_INSERT = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_INSERT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 清除全量报警
export const DATA_SOURCE_CLEAR_FULLPULL_ALARM = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_CLEAR_FULLPULL_ALARM', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])


// Topology kill
export const KILL_TOPOLOGY = createActionTypes('dataSource/dataSourceManager/KILL_TOPOLOGY', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 获取schema list
export const DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 获取schema table
export const DATA_SOURCE_GET_SCHEMA_TABLE_LIST = createActionTypes('dataSource/dataSourceManager/DATA_SOURCE_GET_SCHEMA_TABLE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 清楚schema 和 schema table数据
export const DATA_SOURCE_CLEAN_SCHEMA_TABLE = 'dataSource/dataSourceManager/DATA_SOURCE_CLEAN_SCHEMA_TABLE'

export const OGG_CANAL_CONF_GET_BY_DS_NAME = createActionTypes('dataSource/dataSourceManager/OGG_CANAL_CONF_GET_BY_DS_NAME', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
