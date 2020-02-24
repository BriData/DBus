/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_SINKER_TABLE_LIST,
  SET_SEARCH_SINKER_TABLE_PARAM
} from './types'

// SinkerTable管理查询
export const searchSinkerTableList = {
  request: params => createAction(SEARCH_SINKER_TABLE_LIST.LOAD, { ...params }),
  success: data => createAction(SEARCH_SINKER_TABLE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_SINKER_TABLE_LIST.FAIL, { ...error })
}

export function setSearchSinkerTableParam (params) {
  return {
    type: SET_SEARCH_SINKER_TABLE_PARAM,
    params: params
  }
}
