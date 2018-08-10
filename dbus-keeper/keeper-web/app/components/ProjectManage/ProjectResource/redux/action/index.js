/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_RESOURCE_ALL_PARAMS,
  PROJECT_RESOURCE_ALL_SEARCH,
  PROJECT_RESOURCE_DSNAME_LIST_SEARCH,
  PROJECT_RESOURCE_PROJECT_LIST_SEARCH,
  PROJECT_RESOURCE_TABLE_ENCODE_SEARCH
} from './types'

// Resource管理查询参数
export function setAllResourceParams (params) {
  return {
    type: PROJECT_RESOURCE_ALL_PARAMS,
    params: params
  }
}

// Resource管理查询
export const searchAllResource = {
  request: params => createAction(PROJECT_RESOURCE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_RESOURCE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_RESOURCE_ALL_SEARCH.FAIL, { ...error })
}

export const searchAllResourceProject = {
  request: params => createAction(PROJECT_RESOURCE_PROJECT_LIST_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_RESOURCE_PROJECT_LIST_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_RESOURCE_PROJECT_LIST_SEARCH.FAIL, { ...error })
}

export const searchAllResourceDsName = {
  request: params => createAction(PROJECT_RESOURCE_DSNAME_LIST_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_RESOURCE_DSNAME_LIST_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_RESOURCE_DSNAME_LIST_SEARCH.FAIL, { ...error })
}

export const searchAllResourceTableEncode = {
  request: params => createAction(PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.FAIL, { ...error })
}
