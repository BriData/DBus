/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DATA_TABLE_ALL_PARAMS,
  DATA_TABLE_ALL_SEARCH,
  DATA_TABLE_FIND_ALL_SEARCH,
  DATA_TABLE_GET_ENCODE_CONFIG,
  DATA_TABLE_GET_TABLE_COLUMN,
  DATA_TABLE_GET_ENCODE_TYPE,
  DATA_TABLE_GET_VERSION_LIST,
  DATA_TABLE_GET_VERSION_DETAIL,
  DATA_TABLE_CLEAR_VERSION_DETAIL,
  DATA_TABLE_SOURCE_INSIGHT
} from './types'

export function setDataTableParams (params) {
  return {
    type: DATA_TABLE_ALL_PARAMS,
    params: params
  }
}

export const searchDataTableList = {
  request: params => createAction(DATA_TABLE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_ALL_SEARCH.FAIL, { ...error })
}

export const findAllDataTableList = {
  request: params => createAction(DATA_TABLE_FIND_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_FIND_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_FIND_ALL_SEARCH.FAIL, { ...error })
}

export const getEncodeConfig = {
  request: params => createAction(DATA_TABLE_GET_ENCODE_CONFIG.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_GET_ENCODE_CONFIG.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_GET_ENCODE_CONFIG.FAIL, { ...error })
}

export const getTableColumn = {
  request: params => createAction(DATA_TABLE_GET_TABLE_COLUMN.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_GET_TABLE_COLUMN.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_GET_TABLE_COLUMN.FAIL, { ...error })
}

export const getEncodeType = {
  request: params => createAction(DATA_TABLE_GET_ENCODE_TYPE.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_GET_ENCODE_TYPE.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_GET_ENCODE_TYPE.FAIL, { ...error })
}

export const getVersionList = {
  request: params => createAction(DATA_TABLE_GET_VERSION_LIST.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_GET_VERSION_LIST.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_GET_VERSION_LIST.FAIL, { ...error })
}

export function clearVersionDetail (params) {
  return {
    type: DATA_TABLE_CLEAR_VERSION_DETAIL,
    params: params
  }
}

export const getVersionDetail = {
  request: params => createAction(DATA_TABLE_GET_VERSION_DETAIL.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_GET_VERSION_DETAIL.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_GET_VERSION_DETAIL.FAIL, { ...error })
}

export const getSourceInsight = {
  request: params => createAction(DATA_TABLE_SOURCE_INSIGHT.LOAD, { ...params }),
  success: data => createAction(DATA_TABLE_SOURCE_INSIGHT.SUCCESS, { ...data }),
  fail: error => createAction(DATA_TABLE_SOURCE_INSIGHT.FAIL, { ...error })
}
