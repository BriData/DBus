/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  ALL_USER_LIST_PARAMS,
  ALL_USER_LIST_SEARCH,
  ALL_USER_INFO,
  ALL_USER_PROJECT_INFO
} from './types'

// 用户列表查询参数
export function setUserListParams (params) {
  return {
    type: ALL_USER_LIST_PARAMS,
    params: params
  }
}

// 用户列表查询
export const searchUserList = {
  request: params => createAction(ALL_USER_LIST_SEARCH.LOAD, { ...params }),
  success: data => createAction(ALL_USER_LIST_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(ALL_USER_LIST_SEARCH.FAIL, { ...error })
}
// 获取用户信息
export const getUserInfo = {
  request: params => createAction(ALL_USER_INFO.LOAD, { ...params }),
  success: data => createAction(ALL_USER_INFO.SUCCESS, { ...data }),
  fail: error => createAction(ALL_USER_INFO.FAIL, { ...error })
}
// 获取项目信息
export const getUserProject = {
  request: params => createAction(ALL_USER_PROJECT_INFO.LOAD, { ...params }),
  success: data => createAction(ALL_USER_PROJECT_INFO.SUCCESS, { ...data }),
  fail: error => createAction(ALL_USER_PROJECT_INFO.FAIL, { ...error })
}
