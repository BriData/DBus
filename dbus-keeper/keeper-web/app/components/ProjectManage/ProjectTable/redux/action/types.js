/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// Table管理查询参数
export const PROJECT_TABLE_ALL_PARAMS = 'project/ProjectTable/PROJECT_TABLE_ALL_PARAMS'

// Resource查询参数
export const PROJECT_TABLE_RESOURCE_PARAMS = 'project/ProjectTable/PROJECT_TABLE_RESOURCE_PARAMS'

// 设置项目Sink
export const PROJECT_TABLE_CREATE_SINK = 'project/ProjectTable/PROJECT_TABLE_CREATE_SINK'
// 设置Resource
export const PROJECT_TABLE_CREATE_RESOURCE = 'project/ProjectTable/PROJECT_TABLE_CREATE_RESOURCE'

// 设置拓扑
export const PROJECT_TABLE_CREATE_TOPOLOGY = 'project/ProjectTable/PROJECT_TABLE_CREATE_TOPOLOGY'
// 设置拓扑
export const PROJECT_TABLE_SELECT_ALL_RESOURCE = 'project/ProjectTable/PROJECT_TABLE_SELECT_ALL_RESOURCE'

// 脱敏配置
export const PROJECT_TABLE_CREATE_ENCODES = 'project/ProjectTable/PROJECT_TABLE_CREATE_ENCODES'

// Table管理查询
export const PROJECT_TABLE_ALL_SEARCH = createActionTypes('project/ProjectTable/PROJECT_TABLE_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Table project list
export const PROJECT_TABLE_PROJECT_LIST = createActionTypes('project/ProjectTable/PROJECT_TABLE_PROJECT_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Table Topology list
export const PROJECT_TABLE_TOPOLOGY_LIST = createActionTypes('project/ProjectTable/PROJECT_TABLE_TOPOLOGY_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Table dataSource list
export const PROJECT_TABLE_DATASOURCE_LIST = createActionTypes('project/ProjectTable/PROJECT_TABLE_DATASOURCE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// Table 获取某列table信息
export const PROJECT_GET_TABLEINFO = createActionTypes('project/ProjectTable/PROJECT_GET_TABLEINFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
/** ************新建 */
// 新建  resourceList
export const PROJECT_TABLE_RESOURCELIST = createActionTypes('project/ProjectTable/PROJECT_TABLE_RESOURCELIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 新建  resourceList COLUMNS
export const PROJECT_TABLE_COLUMNS = createActionTypes('project/ProjectTable/PROJECT_TABLE_COLUMNS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 新建  获取Sink list
export const PROJECT_TABLE_SINKS = createActionTypes('project/ProjectTable/PROJECT_TABLE_SINKS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 新建  获取Topic
export const PROJECT_TABLE_TOPICS = createActionTypes('project/ProjectTable/PROJECT_TABLE_TOPICS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 根据topic,获取partition信息
export const PROJECT_TABLE_PARTITIONS = createActionTypes('project/ProjectTable/PROJECT_TABLE_PARTITIONS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 根据topic,tableId 获取受影响的表
export const PROJECT_TABLE_AFFECT_TABLES = createActionTypes('project/ProjectTable/PROJECT_TABLE_AFFECT_TABLES', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 根据tableId 发送Reload消息
export const PROJECT_TABLE_RELOAD = createActionTypes('project/ProjectTable/PROJECT_TABLE_RELOAD', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 新建获取projectID下所有的topo
export const PROJECT_TABLE_ALL_TOPO = createActionTypes('project/ProjectTable/PROJECT_TABLE_ALL_TOPO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
