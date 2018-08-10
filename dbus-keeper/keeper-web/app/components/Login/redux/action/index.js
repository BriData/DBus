/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  LOGIN_SAVE_PARAMS,
  LOGIN_GET_LIST
} from './types'

// 模拟本地存储 action
export function setParams (params) {
  return {
    type: LOGIN_SAVE_PARAMS,
    params: params
  }
}

// 模拟请求 action
export const loginList = {
  request: params => createAction(LOGIN_GET_LIST.LOAD, { ...params }),
  success: data => createAction(LOGIN_GET_LIST.SUCCESS, { ...data }),
  fail: error => createAction(LOGIN_GET_LIST.FAIL, { ...error })
}

