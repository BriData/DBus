/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// Resource管理查询参数
export const PROJECT_RESOURCE_ALL_PARAMS = 'project/ProjectResource/PROJECT_RESOURCE_ALL_PARAMS'
// Resource管理查询
export const PROJECT_RESOURCE_ALL_SEARCH = createActionTypes('project/ProjectResource/PROJECT_RESOURCE_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Resource 查询脱敏
export const PROJECT_RESOURCE_TABLE_ENCODE_SEARCH = createActionTypes('project/ProjectResource/PROJECT_RESOURCE_TABLE_ENCODE_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Resource 项目列表查询
export const PROJECT_RESOURCE_PROJECT_LIST_SEARCH = createActionTypes('project/ProjectResource/PROJECT_RESOURCE_PROJECT_LIST_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Resource dsName列表查询
export const PROJECT_RESOURCE_DSNAME_LIST_SEARCH = createActionTypes('project/ProjectResource/PROJECT_RESOURCE_DSNAME_LIST_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
