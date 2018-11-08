/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_TABLE_ALL_PARAMS,
  PROJECT_TABLE_RESOURCE_PARAMS,
  PROJECT_TABLE_CREATE_SINK,
  PROJECT_TABLE_CREATE_RESOURCE,
  PROJECT_TABLE_CREATE_TOPOLOGY,
  PROJECT_TABLE_SELECT_ALL_RESOURCE,
  PROJECT_TABLE_CREATE_ENCODES,
  PROJECT_TABLE_ALL_SEARCH,
  PROJECT_TABLE_PROJECT_LIST,
  PROJECT_TABLE_TOPOLOGY_LIST,
  PROJECT_TABLE_DATASOURCE_LIST,
  PROJECT_GET_TABLEINFO,
  PROJECT_TABLE_RESOURCELIST,
  PROJECT_TABLE_COLUMNS,
  PROJECT_TABLE_SINKS,
  PROJECT_TABLE_TOPICS,
  PROJECT_TABLE_PARTITIONS,
  PROJECT_TABLE_AFFECT_TABLES,
  PROJECT_TABLE_RELOAD,
  PROJECT_TABLE_ALL_TOPO
} from './types'

// Table管理查询参数
export function setTableParams (params) {
  return {
    type: PROJECT_TABLE_ALL_PARAMS,
    params: params
  }
}

// Resource查询参数
export function setTableResourceParams (params) {
  return {
    type: PROJECT_TABLE_RESOURCE_PARAMS,
    params: params
  }
}

// 设置项目Sink
export function setTableSink (params) {
  return {
    type: PROJECT_TABLE_CREATE_SINK,
    params: params
  }
}
// 设置Resource
export function setTableResource (params) {
  return {
    type: PROJECT_TABLE_CREATE_RESOURCE,
    params: params
  }
}
// 设置拓扑
export function setTableTopology (params) {
  return {
    type: PROJECT_TABLE_CREATE_TOPOLOGY,
    params: params
  }
}
// 选择所有可选表
export function selectAllResource (params) {
  return {
    type: PROJECT_TABLE_SELECT_ALL_RESOURCE,
    params: params
  }
}
// 脱敏配置
export function setTableEncodes (params) {
  return {
    type: PROJECT_TABLE_CREATE_ENCODES,
    params: params
  }
}

// Table管理查询
export const searchTableList = {
  request: params => createAction(PROJECT_TABLE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_ALL_SEARCH.FAIL, { ...error })
}

// Table project list
export const getProjectList = {
  request: params => createAction(PROJECT_TABLE_PROJECT_LIST.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_PROJECT_LIST.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_PROJECT_LIST.FAIL, { ...error })
}
// Table Topology list
export const getTopologyList = {
  request: params => createAction(PROJECT_TABLE_TOPOLOGY_LIST.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_TOPOLOGY_LIST.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_TOPOLOGY_LIST.FAIL, { ...error })
}
// Table dataSource list
export const getDataSourceList = {
  request: params => createAction(PROJECT_TABLE_DATASOURCE_LIST.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_DATASOURCE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_DATASOURCE_LIST.FAIL, { ...error })
}
// Table 获取某列table信息
export const getTableInfo = {
  request: params => createAction(PROJECT_GET_TABLEINFO.LOAD, { ...params }),
  success: data => createAction(PROJECT_GET_TABLEINFO.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_GET_TABLEINFO.FAIL, { ...error })
}
/** ************新建 */

// Table 获取新建 resourceList
export const getResourceList = {
  request: params => createAction(PROJECT_TABLE_RESOURCELIST.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_RESOURCELIST.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_RESOURCELIST.FAIL, { ...error })
}

// Table 获取新建Columns
export const getColumns = {
  request: params => createAction(PROJECT_TABLE_COLUMNS.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_COLUMNS.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_COLUMNS.FAIL, { ...error })
}

// 新建  获取Sink list
export const getTableSinks = {
  request: params => createAction(PROJECT_TABLE_SINKS.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_SINKS.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_SINKS.FAIL, { ...error })
}
// 新建  获取Topic
export const getTableTopics = {
  request: params => createAction(PROJECT_TABLE_TOPICS.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_TOPICS.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_TOPICS.FAIL, { ...error })
}

// 根据topic,获取partition信息
export const getTablePartitions = {
  request: params => createAction(PROJECT_TABLE_PARTITIONS.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_PARTITIONS.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_PARTITIONS.FAIL, { ...error })
}

// 根据topic,tableId 获取受影响的表
export const getTableAffectTables = {
  request: params => createAction(PROJECT_TABLE_AFFECT_TABLES.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_AFFECT_TABLES.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_AFFECT_TABLES.FAIL, { ...error })
}

// 根据tableId 发送Reload消息
export const sendTableReloadMsg = {
  request: params => createAction(PROJECT_TABLE_RELOAD.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_RELOAD.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_RELOAD.FAIL, { ...error })
}

// 新建获取projectID下所有的topo
export const getTableProjectAllTopo = {
  request: params => createAction(PROJECT_TABLE_ALL_TOPO.LOAD, { ...params }),
  success: data => createAction(PROJECT_TABLE_ALL_TOPO.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_TABLE_ALL_TOPO.FAIL, { ...error })
}
