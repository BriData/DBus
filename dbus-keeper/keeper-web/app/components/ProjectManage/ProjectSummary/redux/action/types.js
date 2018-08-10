/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 启用禁用项目
export const ENABLE_DISABLE_PROJECT = createActionTypes('project/ProjectHome/ProjectSummary/ENABLE_DISABLE_PROJECT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 删除项目
export const DELETE_PROJECT = createActionTypes('project/ProjectHome/ProjectSummary/DELETE_PROJECT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 获取项目列表
export const PROJECT_GET_LIST = createActionTypes('project/ProjectHome/ProjectSummary/PROJECT_LIST_ALL', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
