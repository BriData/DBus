/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_GET_LIST,
  DELETE_PROJECT,
  ENABLE_DISABLE_PROJECT
} from './types'

// 启用禁用项目
export const enableDisableProject = {
  request: params => createAction(ENABLE_DISABLE_PROJECT.LOAD, { ...params }),
  success: data => createAction(ENABLE_DISABLE_PROJECT.SUCCESS, { ...data }),
  fail: error => createAction(ENABLE_DISABLE_PROJECT.FAIL, { ...error })
}

// 删除项目
export const deleteProject = {
  request: params => createAction(DELETE_PROJECT.LOAD, { ...params }),
  success: data => createAction(DELETE_PROJECT.SUCCESS, { ...data }),
  fail: error => createAction(DELETE_PROJECT.FAIL, { ...error })
}

// 获取项目列表
export const searchProject = {
  request: params => createAction(PROJECT_GET_LIST.LOAD, { ...params }),
  success: data => createAction(PROJECT_GET_LIST.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_GET_LIST.FAIL, { ...error })
}
