/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_USER_PARAMS,
  PROJECT_SINK_PARAMS,
  PROJECT_RESOURCE_PARAMS,
  PROJECT_CREATE_BASICINFO,
  PROJECT_CREATE_USER,
  PROJECT_CREATE_SINK,
  PROJECT_CREATE_RESOURCE,
  PROJECT_CREATE_ALARM,
  PROJECT_CREATE_ENCODES,
  PROJECT_USER_SEARCH,
  PROJECT_SINK_SEARCH,
  PROJECT_RESOURCE_SEARCH,
  RESOURCE_ENCODE_LIST,
  RESOURCE_ENCODE_TYPE_LIST,
  GET_PROJECT_INFO
} from './types'

// 设置项目基本信息
export function setBasicInfo (params) {
  return {
    type: PROJECT_CREATE_BASICINFO,
    params: params
  }
}
// 设置项目用户
export function setUser (params) {
  return {
    type: PROJECT_CREATE_USER,
    params: params
  }
}
// 设置项目Sink
export function setSink (params) {
  return {
    type: PROJECT_CREATE_SINK,
    params: params
  }
}
// 设置Resource
export function setResource (params) {
  return {
    type: PROJECT_CREATE_RESOURCE,
    params: params
  }
}
// 设置报警策略
export function setAlarm (params) {
  return {
    type: PROJECT_CREATE_ALARM,
    params: params
  }
}
// 脱敏配置
export function setEncodes (params) {
  return {
    type: PROJECT_CREATE_ENCODES,
    params: params
  }
}
// 用户管理查询参数
export function setUserParams (params) {
  return {
    type: PROJECT_USER_PARAMS,
    params: params
  }
}
// sink管理查询参数
export function setSinkParams (params) {
  return {
    type: PROJECT_SINK_PARAMS,
    params: params
  }
}
// Resource管理查询参数
export function setResourceParams (params) {
  return {
    type: PROJECT_RESOURCE_PARAMS,
    params: params
  }
}
// 用户管理查询
export const searchUser = {
  request: params => createAction(PROJECT_USER_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_USER_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_USER_SEARCH.FAIL, { ...error })
}
// sink管理查询
export const searchSink = {
  request: params => createAction(PROJECT_SINK_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_SINK_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_SINK_SEARCH.FAIL, { ...error })
}
// Resource管理查询
export const searchResource = {
  request: params => createAction(PROJECT_RESOURCE_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_RESOURCE_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_RESOURCE_SEARCH.FAIL, { ...error })
}
// 获取脱敏配置列表
export const searchEncode = {
  request: params => createAction(RESOURCE_ENCODE_LIST.LOAD, { ...params }),
  success: data => createAction(RESOURCE_ENCODE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(RESOURCE_ENCODE_LIST.FAIL, { ...error })
}
// 脱敏规则-下拉列表
export const getEncodeTypeList = {
  request: params => createAction(RESOURCE_ENCODE_TYPE_LIST.LOAD, { ...params }),
  success: data => createAction(RESOURCE_ENCODE_TYPE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(RESOURCE_ENCODE_TYPE_LIST.FAIL, { ...error })
}

// 获取项目信息
export const getProjectInfo = {
  request: params => createAction(GET_PROJECT_INFO.LOAD, { ...params }),
  success: data => createAction(GET_PROJECT_INFO.SUCCESS, { ...data }),
  fail: error => createAction(GET_PROJECT_INFO.FAIL, { ...error })
}
