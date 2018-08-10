/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DATA_SCHEMA_ALL_PARAMS,
  DATA_SCHEMA_ALL_SEARCH,
  DATA_SCHEMA_SEARCH_ALL_LIST,
  DATA_SCHEMA_GET_BY_ID,
} from './types'

export function setDataSchemaParams (params) {
  return {
    type: DATA_SCHEMA_ALL_PARAMS,
    params: params
  }
}

export const searchDataSchemaList = {
  request: params => createAction(DATA_SCHEMA_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DATA_SCHEMA_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SCHEMA_ALL_SEARCH.FAIL, { ...error })
}

// DataSource 管理查询
export const getDataSchemaById = {
  request: params => createAction(DATA_SCHEMA_GET_BY_ID.LOAD, { ...params }),
  success: data => createAction(DATA_SCHEMA_GET_BY_ID.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SCHEMA_GET_BY_ID.FAIL, { ...error })
}

// DataSchema搜索所有信息，不需要分页
export const searchAllDataSchema = {
  request: params => createAction(DATA_SCHEMA_SEARCH_ALL_LIST.LOAD, { ...params }),
  success: data => createAction(DATA_SCHEMA_SEARCH_ALL_LIST.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SCHEMA_SEARCH_ALL_LIST.FAIL, { ...error })
}
