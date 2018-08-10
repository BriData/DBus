/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// FullpullHistory管理查询参数
export const PROJECT_FULLPULL_HISTORY_ALL_PARAMS = 'project/ProjectFullpull/PROJECT_FULLPULL_HISTORY_ALL_PARAMS'
// FullpullHistory管理查询
export const PROJECT_FULLPULL_HISTORY_ALL_SEARCH = createActionTypes('project/ProjectFullpull/PROJECT_FULLPULL_HISTORY_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// FullpullHistory 项目列表查询
export const PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH = createActionTypes('project/ProjectFullpull/PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// FullpullHistory dsName列表查询
export const PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH = createActionTypes('project/ProjectFullpull/PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
