/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_SINKER_SCHEMA_LIST,
  SET_SEARCH_SINKER_SCHEMA_PARAM
} from './types'

// SinkerSchema管理查询
export const searchSinkerSchemaList = {
  request: params => createAction(SEARCH_SINKER_SCHEMA_LIST.LOAD, { ...params }),
  success: data => createAction(SEARCH_SINKER_SCHEMA_LIST.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_SINKER_SCHEMA_LIST.FAIL, { ...error })
}

export function setSearchSinkerSchemaParam (params) {
  return {
    type: SET_SEARCH_SINKER_SCHEMA_PARAM,
    params: params
  }
}
