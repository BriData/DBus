/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
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
} from '../action/types'

const initialState = fromJS({
  projectTableStorage: {
    sink: null,
    resource: null,
    topology: null,
    encodes: null
  },
  tableList: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  topologyList: {
    loading: false,
    loaded: false,
    result: {}
  },
  projectList: {
    loading: false,
    loaded: false,
    result: {}
  },
  tableInfo: {
    loading: false,
    loaded: false,
    result: {}
  },
  resourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  columnsList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sinkList: {
    loading: false,
    loaded: false,
    result: {}
  },
  topicList: {
    loading: false,
    loaded: false,
    result: {}
  },
  partitionList: {
    loading: false,
    loaded: false,
    result: {}
  },
  affectTableList: {
    loading: false,
    loaded: false,
    result: {}
  },
  reloadResponse: {
    loading: false,
    loaded: false,
    result: {}
  },
  projectTopos: {
    loading: false,
    loaded: false,
    result: {}
  },
  tableParams: null,
  resourceParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // Table管理查询
    case PROJECT_TABLE_ALL_SEARCH.LOAD:
      return state.setIn(['tableList', 'loading'], true)
    case PROJECT_TABLE_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['tableList', 'loading'], false)
        .setIn(['tableList', 'loaded'], true)
        .setIn(['tableList', 'result'], action.result)
    case PROJECT_TABLE_ALL_SEARCH.FAIL:
      return state
        .setIn(['tableList', 'loading'], false)
        .setIn(['tableList', 'loaded'], true)
        .setIn(['tableList', 'result'], action.result)
    // dataSourceList
    case PROJECT_TABLE_DATASOURCE_LIST.LOAD:
      return state.setIn(['dataSourceList', 'loading'], true)
    case PROJECT_TABLE_DATASOURCE_LIST.SUCCESS:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    case PROJECT_TABLE_DATASOURCE_LIST.FAIL:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    // projectList
    case PROJECT_TABLE_PROJECT_LIST.LOAD:
      return state.setIn(['projectList', 'loading'], true)
    case PROJECT_TABLE_PROJECT_LIST.SUCCESS:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    case PROJECT_TABLE_PROJECT_LIST.FAIL:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    // topologyList
    case PROJECT_TABLE_TOPOLOGY_LIST.LOAD:
      return state.setIn(['topologyList', 'loading'], true)
    case PROJECT_TABLE_TOPOLOGY_LIST.SUCCESS:
      return state
        .setIn(['topologyList', 'loading'], false)
        .setIn(['topologyList', 'loaded'], true)
        .setIn(['topologyList', 'result'], action.result)
    case PROJECT_TABLE_TOPOLOGY_LIST.FAIL:
      return state
        .setIn(['topologyList', 'loading'], false)
        .setIn(['topologyList', 'loaded'], true)
        .setIn(['topologyList', 'result'], action.result)
    // tableInfo
    case PROJECT_GET_TABLEINFO.LOAD:
      return state.setIn(['tableInfo', 'loading'], true)
    case PROJECT_GET_TABLEINFO.SUCCESS:
      return state
        .setIn(['tableInfo', 'loading'], false)
        .setIn(['tableInfo', 'loaded'], true)
        .setIn(['tableInfo', 'result'], action.result)
    case PROJECT_GET_TABLEINFO.FAIL:
      return state
        .setIn(['tableInfo', 'loading'], false)
        .setIn(['tableInfo', 'loaded'], true)
        .setIn(['tableInfo', 'result'], action.result)
    // 新建  resourceList
    case PROJECT_TABLE_RESOURCELIST.LOAD:
      return state.setIn(['resourceList', 'loading'], true)
    case PROJECT_TABLE_RESOURCELIST.SUCCESS:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)
    case PROJECT_TABLE_RESOURCELIST.FAIL:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)
    // 新建  resourceList COLUMNS
    case PROJECT_TABLE_COLUMNS.LOAD:
      return state.setIn(['columnsList', 'loading'], true)
    case PROJECT_TABLE_COLUMNS.SUCCESS:
      return state
        .setIn(['columnsList', 'loading'], false)
        .setIn(['columnsList', 'loaded'], true)
        .setIn(['columnsList', 'result'], action.result)
    case PROJECT_TABLE_COLUMNS.FAIL:
      return state
        .setIn(['columnsList', 'loading'], false)
        .setIn(['columnsList', 'loaded'], true)
        .setIn(['columnsList', 'result'], action.result)
    // 新建  获取Sink list
    case PROJECT_TABLE_SINKS.LOAD:
      return state.setIn(['sinkList', 'loading'], true)
    case PROJECT_TABLE_SINKS.SUCCESS:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    case PROJECT_TABLE_SINKS.FAIL:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    // 新建  获取Topic
    case PROJECT_TABLE_TOPICS.LOAD:
      return state.setIn(['topicList', 'loading'], true)
    case PROJECT_TABLE_TOPICS.SUCCESS:
      return state
        .setIn(['topicList', 'loading'], false)
        .setIn(['topicList', 'loaded'], true)
        .setIn(['topicList', 'result'], action.result)
    case PROJECT_TABLE_TOPICS.FAIL:
      return state
        .setIn(['topicList', 'loading'], false)
        .setIn(['topicList', 'loaded'], true)
        .setIn(['topicList', 'result'], action.result)
    // 根据topic,获取partition信息
    case PROJECT_TABLE_PARTITIONS.LOAD:
      return state.setIn(['partitionList', 'loading'], true)
    case PROJECT_TABLE_PARTITIONS.SUCCESS:
      return state
        .setIn(['partitionList', 'loading'], false)
        .setIn(['partitionList', 'loaded'], true)
        .setIn(['partitionList', 'result'], action.result)
    case PROJECT_TABLE_PARTITIONS.FAIL:
      return state
        .setIn(['partitionList', 'loading'], false)
        .setIn(['partitionList', 'loaded'], true)
        .setIn(['partitionList', 'result'], action.result)
    // 根据topic,tableId 获取受影响的表
    case PROJECT_TABLE_AFFECT_TABLES.LOAD:
      return state.setIn(['affectTableList', 'loading'], true)
    case PROJECT_TABLE_AFFECT_TABLES.SUCCESS:
      return state
        .setIn(['affectTableList', 'loading'], false)
        .setIn(['affectTableList', 'loaded'], true)
        .setIn(['affectTableList', 'result'], action.result)
    case PROJECT_TABLE_AFFECT_TABLES.FAIL:
      return state
        .setIn(['affectTableList', 'loading'], false)
        .setIn(['affectTableList', 'loaded'], true)
        .setIn(['affectTableList', 'result'], action.result)
    // 根据tableId 发送Reload消息
    case PROJECT_TABLE_RELOAD.LOAD:
      return state.setIn(['reloadResponse', 'loading'], true)
    case PROJECT_TABLE_RELOAD.SUCCESS:
      return state
        .setIn(['reloadResponse', 'loading'], false)
        .setIn(['reloadResponse', 'loaded'], true)
        .setIn(['reloadResponse', 'result'], action.result)
    case PROJECT_TABLE_RELOAD.FAIL:
      return state
        .setIn(['reloadResponse', 'loading'], false)
        .setIn(['reloadResponse', 'loaded'], true)
        .setIn(['reloadResponse', 'result'], action.result)
    // 新建获取projectID下所有的topo
    case PROJECT_TABLE_ALL_TOPO.LOAD:
      return state.setIn(['projectTopos', 'loading'], true)
    case PROJECT_TABLE_ALL_TOPO.SUCCESS:
      return state
        .setIn(['projectTopos', 'loading'], false)
        .setIn(['projectTopos', 'loaded'], true)
        .setIn(['projectTopos', 'result'], action.result)
    case PROJECT_TABLE_ALL_TOPO.FAIL:
      return state
        .setIn(['projectTopos', 'loading'], false)
        .setIn(['projectTopos', 'loaded'], true)
        .setIn(['projectTopos', 'result'], action.result)
    // Table管理查询参数
    case PROJECT_TABLE_ALL_PARAMS:
      return state.setIn(['tableParams'], action.params)
    // Table管理查询参数
    case PROJECT_TABLE_RESOURCE_PARAMS:
      return state.setIn(['resourceParams'], action.params)
    // 设置项目Sink
    case PROJECT_TABLE_CREATE_SINK:
      return state.setIn(['projectTableStorage', 'sink'], action.params)
    // 设置Resource
    case PROJECT_TABLE_CREATE_RESOURCE:
      return state.setIn(['projectTableStorage', 'resource'], action.params)
    // 设置拓扑
    case PROJECT_TABLE_CREATE_TOPOLOGY:
      return state.setIn(['projectTableStorage', 'topology'], action.params)
    // 选择所有资源
    case PROJECT_TABLE_SELECT_ALL_RESOURCE:
      return state.setIn(['projectTableStorage', 'isSelectAllResource'], action.params)
    // 脱敏配置
    case PROJECT_TABLE_CREATE_ENCODES:
      return state.setIn(['projectTableStorage', 'encodes'], action.params)
    default:
      return state
  }
}
