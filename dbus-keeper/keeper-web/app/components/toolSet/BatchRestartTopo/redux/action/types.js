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

// Topology kill
export const KILL_TOPOLOGY = createActionTypes('dataSource/dataSourceManager/KILL_TOPOLOGY', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSource 清楚schema 和 schema table数据
export const DATA_SOURCE_CLEAN_SCHEMA_TABLE = 'dataSource/dataSourceManager/DATA_SOURCE_CLEAN_SCHEMA_TABLE'
