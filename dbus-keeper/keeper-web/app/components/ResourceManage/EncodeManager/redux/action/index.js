/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  ENCODE_MANAGER_SEARCH,
  ENCODE_MANAGER_SET_PARAM
} from './types'

export function setEncodeManagerParams (params) {
  return {
    type: ENCODE_MANAGER_SET_PARAM,
    params: params
  }
}

export const searchEncodeList = {
  request: params => createAction(ENCODE_MANAGER_SEARCH.LOAD, { ...params }),
  success: data => createAction(ENCODE_MANAGER_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(ENCODE_MANAGER_SEARCH.FAIL, { ...error })
}
