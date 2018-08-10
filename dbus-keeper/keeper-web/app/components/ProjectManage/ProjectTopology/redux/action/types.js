/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// Topology管理查询参数
export const PROJECT_TOPOLOGY_ALL_PARAMS = 'project/ProjectTopology/PROJECT_TOPOLOGY_ALL_PARAMS'
// Topology管理查询
export const PROJECT_TOPOLOGY_ALL_SEARCH = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取某项 Topology 信息
export const PROJECT_TOPOLOGY_INFO = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_INFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取开始信息
export const PROJECT_TOPOLOGY_START_INFO_SEARCH = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_START_INFO_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取停止日志
export const PROJECT_TOPOLOGY_STOP_INFO_SEARCH = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_STOP_INFO_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取订阅源
export const PROJECT_TOPOLOGY_FEEDS_SEARCH = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_FEEDS_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取输出topic列表
export const PROJECT_TOPOLOGY_OUTTOPIC_SEARCH = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_OUTTOPIC_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取Jar版本
export const PROJECT_TOPOLOGY_JAR_VERSIONS = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_JAR_VERSIONS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 获取Jar包
export const PROJECT_TOPOLOGY_JAR_PACKAGES = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_JAR_PACKAGES', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Topology 生效
export const PROJECT_TOPOLOGY_EFFECT = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_EFFECT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Topology 获取当前offset
export const PROJECT_TOPOLOGY_RERUN_INIT = createActionTypes('project/ProjectTopology/PROJECT_TOPOLOGY_RERUN_INIT', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
