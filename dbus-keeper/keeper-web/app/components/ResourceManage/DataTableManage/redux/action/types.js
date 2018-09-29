/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// DataTable管理查询参数
export const DATA_TABLE_ALL_PARAMS = 'dataTable/dataTableManager/DATA_TABLE_ALL_PARAMS'

// DataTable管理查询
export const DATA_TABLE_ALL_SEARCH = createActionTypes('dataTable/dataTableManager/DATA_TABLE_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 查询所有table 不分页
export const DATA_TABLE_FIND_ALL_SEARCH = createActionTypes('dataTable/dataTableManager/DATA_TABLE_FIND_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_GET_ENCODE_CONFIG = createActionTypes('dataTable/dataTableManager/DATA_TABLE_GET_ENCODE_CONFIG', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_GET_TABLE_COLUMN = createActionTypes('dataTable/dataTableManager/DATA_TABLE_GET_TABLE_COLUMN', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_GET_ENCODE_TYPE = createActionTypes('dataTable/dataTableManager/DATA_TABLE_GET_ENCODE_TYPE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_GET_VERSION_LIST = createActionTypes('dataTable/dataTableManager/DATA_TABLE_GET_VERSION_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_CLEAR_VERSION_DETAIL = 'dataTable/dataTableManager/DATA_TABLE_CLEAR_VERSION_DETAIL'

export const DATA_TABLE_GET_VERSION_DETAIL = createActionTypes('dataTable/dataTableManager/DATA_TABLE_GET_VERSION_DETAIL', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const DATA_TABLE_SOURCE_INSIGHT = createActionTypes('dataTable/dataTableManager/DATA_TABLE_SOURCE_INSIGHT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])



