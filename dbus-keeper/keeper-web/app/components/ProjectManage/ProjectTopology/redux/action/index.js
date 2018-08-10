/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_TOPOLOGY_ALL_PARAMS,
  PROJECT_TOPOLOGY_ALL_SEARCH,
  PROJECT_TOPOLOGY_INFO,
  PROJECT_TOPOLOGY_START_INFO_SEARCH,
  PROJECT_TOPOLOGY_STOP_INFO_SEARCH,
  PROJECT_TOPOLOGY_FEEDS_SEARCH,
  PROJECT_TOPOLOGY_OUTTOPIC_SEARCH,
  PROJECT_TOPOLOGY_JAR_VERSIONS,
  PROJECT_TOPOLOGY_JAR_PACKAGES,
  PROJECT_TOPOLOGY_EFFECT,
  PROJECT_TOPOLOGY_RERUN_INIT
} from './types'

// Topology管理查询参数
export function setAllTopologyParams (params) {
  return {
    type: PROJECT_TOPOLOGY_ALL_PARAMS,
    params: params
  }
}

// Topology管理查询
export const searchAllTopology = {
  request: params => createAction(PROJECT_TOPOLOGY_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_ALL_SEARCH.FAIL, { ...error })
}
// Topology 获取某项 Topology 信息
export const getTopologyInfo = {
  request: params => createAction(PROJECT_TOPOLOGY_INFO.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_INFO.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_INFO.FAIL, { ...error })
}
// Topology 获取开始信息
export const getTopologyStartInfo = {
  request: params => createAction(PROJECT_TOPOLOGY_START_INFO_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_START_INFO_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_START_INFO_SEARCH.FAIL, { ...error })
}
// Topology 获取停止日志
export const getTopologyStopInfo = {
  request: params => createAction(PROJECT_TOPOLOGY_STOP_INFO_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_STOP_INFO_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_STOP_INFO_SEARCH.FAIL, { ...error })
}
// Topology 获取订阅源
export const getTopologyFeeds = {
  request: params => createAction(PROJECT_TOPOLOGY_FEEDS_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_FEEDS_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_FEEDS_SEARCH.FAIL, { ...error })
}
// Topology 获取输出topic列表
export const getTopologyOutTopic = {
  request: params => createAction(PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.FAIL, { ...error })
}
// Topology 获取Jar版本
export const getTopologyJarVersions = {
  request: params => createAction(PROJECT_TOPOLOGY_JAR_VERSIONS.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_JAR_VERSIONS.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_JAR_VERSIONS.FAIL, { ...error })
}
// Topology 获取Jar包
export const getTopologyJarPackages = {
  request: params => createAction(PROJECT_TOPOLOGY_JAR_PACKAGES.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_JAR_PACKAGES.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_JAR_PACKAGES.FAIL, { ...error })
}

// Topology 生效
export const topologyEffect = {
  request: params => createAction(PROJECT_TOPOLOGY_EFFECT.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_EFFECT.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_EFFECT.FAIL, { ...error })
}

export const topologyRerunInit = {
  request: params => createAction(PROJECT_TOPOLOGY_RERUN_INIT.LOAD, { ...params }),
  success: data => createAction(PROJECT_TOPOLOGY_RERUN_INIT.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TOPOLOGY_RERUN_INIT.FAIL, { ...error })
}
