/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
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
} from '../action/types'

const initialState = fromJS({
  projectStorage: {
    basic: null,
    user: null,
    sink: null,
    resource: null,
    alarm: null,
    encodes: null
  },
  userList: {
    loading: false,
    loaded: false,
    result: {}
  },
  userParams: null,
  sinkList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sinkParams: null,
  resourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  resourceParams: null,
  encodeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  encodeTypeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  projectInfo: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 设置项目基本信息
    case PROJECT_CREATE_BASICINFO:
      return state.setIn(['projectStorage', 'basic'], action.params)
    // 设置项目用户
    case PROJECT_CREATE_USER:
      return state.setIn(['projectStorage', 'user'], action.params)
    // 设置项目Sink
    case PROJECT_CREATE_SINK:
      return state.setIn(['projectStorage', 'sink'], action.params)
    // 设置Resource
    case PROJECT_CREATE_RESOURCE:
      return state.setIn(['projectStorage', 'resource'], action.params)
    // 设置报警策略
    case PROJECT_CREATE_ALARM:
      return state.setIn(['projectStorage', 'alarm'], action.params)
    // 脱敏配置
    case PROJECT_CREATE_ENCODES:
      return state.setIn(['projectStorage', 'encodes'], action.params)
    // 获取项目信息
    case GET_PROJECT_INFO.LOAD:
      return state.setIn(['projectInfo', 'loading'], true)
    case GET_PROJECT_INFO.SUCCESS:
      return state
        .setIn(['projectInfo', 'loading'], false)
        .setIn(['projectInfo', 'loaded'], true)
        .setIn(['projectInfo', 'result'], action.result)
    case GET_PROJECT_INFO.FAIL:
      return state
        .setIn(['projectInfo', 'loading'], false)
        .setIn(['projectInfo', 'loaded'], true)
        .setIn(['projectInfo', 'result'], action.result)
    // 用户管理查询
    case PROJECT_USER_SEARCH.LOAD:
      return state.setIn(['userList', 'loading'], true)
    case PROJECT_USER_SEARCH.SUCCESS:
      return state
        .setIn(['userList', 'loading'], false)
        .setIn(['userList', 'loaded'], true)
        .setIn(['userList', 'result'], action.result)
    case PROJECT_USER_SEARCH.FAIL:
      return state
        .setIn(['userList', 'loading'], false)
        .setIn(['userList', 'loaded'], true)
        .setIn(['userList', 'result'], action.result)
    // 用户管理查询参数
    case PROJECT_USER_PARAMS:
      return state.setIn(['userParams'], action.params)
    // sink管理查询
    case PROJECT_SINK_SEARCH.LOAD:
      return state.setIn(['sinkList', 'loading'], true)
    case PROJECT_SINK_SEARCH.SUCCESS:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    case PROJECT_SINK_SEARCH.FAIL:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    // sink管理查询参数
    case PROJECT_SINK_PARAMS:
      return state.setIn(['sinkParams'], action.params)
    // Resource管理查询
    case PROJECT_RESOURCE_SEARCH.LOAD:
      return state.setIn(['resourceList', 'loading'], true)
    case PROJECT_RESOURCE_SEARCH.SUCCESS:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)
    case PROJECT_RESOURCE_SEARCH.FAIL:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)
    // Resource管理查询参数
    case PROJECT_RESOURCE_PARAMS:
      return state.setIn(['resourceParams'], action.params)
    // 获取脱敏配置列表
    case RESOURCE_ENCODE_LIST.LOAD:
      return state.setIn(['encodeList', 'loading'], true)
    case RESOURCE_ENCODE_LIST.SUCCESS:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)
    case RESOURCE_ENCODE_LIST.FAIL:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)
    // 获取 脱敏规则-下拉列表
    case RESOURCE_ENCODE_TYPE_LIST.LOAD:
      return state.setIn(['encodeTypeList', 'loading'], true)
    case RESOURCE_ENCODE_TYPE_LIST.SUCCESS:
      return state
        .setIn(['encodeTypeList', 'loading'], false)
        .setIn(['encodeTypeList', 'loaded'], true)
        .setIn(['encodeTypeList', 'result'], action.result)
    case RESOURCE_ENCODE_TYPE_LIST.FAIL:
      return state
        .setIn(['encodeTypeList', 'loading'], false)
        .setIn(['encodeTypeList', 'loaded'], true)
        .setIn(['encodeTypeList', 'result'], action.result)
    default:
      return state
  }
}
