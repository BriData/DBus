/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 用户列表查询参数
export const ALL_USER_LIST_PARAMS = 'UserManage/ALL_USER_LIST_PARAMS'
// 用户列表查询
export const ALL_USER_LIST_SEARCH = createActionTypes('UserManage/ALL_USER_LIST_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 获取用户信息
export const ALL_USER_INFO = createActionTypes('UserManage/ALL_USER_INFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 获取项目信息
export const ALL_USER_PROJECT_INFO = createActionTypes('UserManage/ALL_USER_PROJECT_INFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
