/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// DataSchema管理查询参数
export const DATA_SCHEMA_ALL_PARAMS = 'dataSchema/dataSchemaManager/DATA_SCHEMA_ALL_PARAMS'

// DataSchema管理查询
export const DATA_SCHEMA_ALL_SEARCH = createActionTypes('dataSchema/dataSchemaManager/DATA_SCHEMA_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSchema搜索所有信息，不需要分页
export const DATA_SCHEMA_SEARCH_ALL_LIST = createActionTypes('dataSchema/dataSchemaManager/DATA_SCHEMA_SEARCH_ALL_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// DataSchema管理查询
export const DATA_SCHEMA_GET_BY_ID = createActionTypes('dataSchema/dataSchemaManager/DATA_SCHEMA_GET_BY_ID', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
