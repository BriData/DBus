/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DATA_SOURCE_ALL_PARAMS,
  DATA_SOURCE_ALL_SEARCH,
  DATA_SOURCE_GET_ID_TYPE_NAME,
  DATA_SOURCE_GET_BY_ID,
  KILL_TOPOLOGY
} from './types'

// DataSource 管理查询参数
export function setDataSourceParams (params) {
  return {
    type: DATA_SOURCE_ALL_PARAMS,
    params: params
  }
}
// DataSource 管理查询
export const searchDatasourceList = {
  request: params => createAction(DATA_SOURCE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_ALL_SEARCH.FAIL, { ...error })
}

// DataSource名称和id的列表，提供给schema级应用使用
export const searchDataSourceIdTypeName = {
  request: params => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.FAIL, { ...error })
}

// DataSource 管理查询
export const getDataSourceById = {
  request: params => createAction(DATA_SOURCE_GET_BY_ID.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_BY_ID.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_BY_ID.FAIL, { ...error })
}

// topology kill
export const killTopology = {
  request: params => createAction(KILL_TOPOLOGY.LOAD, { ...params }),
  success: data => createAction(KILL_TOPOLOGY.SUCCESS, { ...data }),
  fail: error => createAction(KILL_TOPOLOGY.FAIL, { ...error })
}


