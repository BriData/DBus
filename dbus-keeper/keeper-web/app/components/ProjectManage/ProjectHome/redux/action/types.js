/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 模拟本地存储
export const PROJECT_SAVE_PARAMS = 'project/ProjectHome/ProjectEditedModal/PROJECT_SAVE_PARAMS'

// 项目基本信息type
export const PROJECT_CREATE_BASICINFO = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_BASICINFO'
// 项目用户管理type
export const PROJECT_CREATE_USER = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_USER'
// 项目Sink管理type
export const PROJECT_CREATE_SINK = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_SINK'
// 项目Resource管理 type
export const PROJECT_CREATE_RESOURCE = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_RESOURCE'
// 项目报警策略设置type
export const PROJECT_CREATE_ALARM = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_ALARM'
// 项目脱敏配置 type
export const PROJECT_CREATE_ENCODES = 'project/ProjectHome/ProjectEditedModal/PROJECT_CREATE_ENCODES'
// 新增项目
export const ADD_PROJECT = createActionTypes('project/ProjectHome/ADD_PROJECT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 更新项目
export const UPDATE_PROJECT = createActionTypes('project/ProjectHome/ENABLE_DISABLE_PROJECT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 用户管理查询参数
export const PROJECT_USER_PARAMS = 'project/ProjectHome/PROJECT_USER_PARAMS'
// 用户管理查询
export const PROJECT_USER_SEARCH = createActionTypes('project/ProjectHome/PROJECT_USER_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// sink管理查询参数
export const PROJECT_SINK_PARAMS = 'project/ProjectHome/PROJECT_SINK_PARAMS'
// sink管理查询
export const PROJECT_SINK_SEARCH = createActionTypes('project/ProjectHome/PROJECT_SINK_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Resource管理查询参数
export const PROJECT_RESOURCE_PARAMS = 'project/ProjectHome/PROJECT_RESOURCE_PARAMS'
// Resource管理查询
export const PROJECT_RESOURCE_SEARCH = createActionTypes('project/ProjectHome/PROJECT_RESOURCE_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 获取脱敏配置列表
export const RESOURCE_ENCODE_LIST = createActionTypes('project/ProjectHome/RESOURCE_ENCODE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 脱敏规则-下拉列表
export const RESOURCE_ENCODE_TYPE_LIST = createActionTypes('project/ProjectHome/RESOURCE_ENCODE_TYPE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 获取项目信息
export const GET_PROJECT_INFO = createActionTypes('project/ProjectHome/ProjectSummary/GET_PROJECT_INFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
