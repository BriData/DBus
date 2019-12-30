/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DATA_SOURCE_ALL_PARAMS,
  DATA_SOURCE_ALL_SEARCH,
  DATA_SOURCE_GET_ID_TYPE_NAME,
  DATA_SOURCE_GET_BY_ID,
  DATA_SOURCE_DELETE,
  DATA_SOURCE_UPDATE,
  DATA_SOURCE_INSERT,
  KILL_TOPOLOGY,
  DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID,
  DATA_SOURCE_GET_SCHEMA_TABLE_LIST,
  DATA_SOURCE_CLEAN_SCHEMA_TABLE,
  DATA_SOURCE_CLEAR_FULLPULL_ALARM,
  OGG_CANAL_CONF_GET_BY_DS_NAME
} from '../action/types'

const initialState = fromJS({
  jarInfos: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceIdTypeName: {
    loading: false,
    loaded: false,
    result: {}
  },
  theDataSourceGottenById: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceDelete: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceUpdate: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceInsert: {
    loading: false,
    loaded: false,
    result: {}
  },
  killTopologyResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  schemaList: {
    loading: false,
    loaded: false,
    result: {}
  },
  schemaTableResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  clearFullPullAlarmResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  oggCanalConfDsName: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceParams: null,
  jarParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    case DATA_SOURCE_CLEAR_FULLPULL_ALARM.LOAD:
      return state.setIn(['clearFullPullAlarmResult','loading'],true)
    case DATA_SOURCE_CLEAR_FULLPULL_ALARM.SUCCESS:
      return state
        .setIn(['clearFullPullAlarmResult', 'loading'], false)
        .setIn(['clearFullPullAlarmResult', 'loaded'], true)
        .setIn(['clearFullPullAlarmResult', 'result'], action.result)
    case DATA_SOURCE_CLEAR_FULLPULL_ALARM.FAIL:
      return state
        .setIn(['clearFullPullAlarmResult', 'loading'], false)
        .setIn(['clearFullPullAlarmResult', 'loaded'], true)
        .setIn(['clearFullPullAlarmResult', 'result'], action.result)
    // dataSource 管理查询
    case DATA_SOURCE_ALL_SEARCH.LOAD:
      return state.setIn(['dataSourceList','loading'],true)
    case DATA_SOURCE_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    case DATA_SOURCE_ALL_SEARCH.FAIL:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    // DataSource名称和id的列表，提供给schema级应用使用
    case DATA_SOURCE_GET_ID_TYPE_NAME.LOAD:
      return state.setIn(['dataSourceIdTypeName','loading'],true)
    case DATA_SOURCE_GET_ID_TYPE_NAME.SUCCESS:
      return state
        .setIn(['dataSourceIdTypeName', 'loading'], false)
        .setIn(['dataSourceIdTypeName', 'loaded'], true)
        .setIn(['dataSourceIdTypeName', 'result'], action.result)
    case DATA_SOURCE_GET_ID_TYPE_NAME.FAIL:
      return state
        .setIn(['dataSourceIdTypeName', 'loading'], false)
        .setIn(['dataSourceIdTypeName', 'loaded'], true)
        .setIn(['dataSourceIdTypeName', 'result'], action.result)
    // dataSource 管理查询
    case DATA_SOURCE_GET_BY_ID.LOAD:
      return state.setIn(['theDataSourceGottenById','loading'],true)
    case DATA_SOURCE_GET_BY_ID.SUCCESS:
      return state
        .setIn(['theDataSourceGottenById', 'loading'], false)
        .setIn(['theDataSourceGottenById', 'loaded'], true)
        .setIn(['theDataSourceGottenById', 'result'], action.result)
    case DATA_SOURCE_GET_BY_ID.FAIL:
      return state
        .setIn(['theDataSourceGottenById', 'loading'], false)
        .setIn(['theDataSourceGottenById', 'loaded'], true)
        .setIn(['theDataSourceGottenById', 'result'], action.result)
    // dataSource 删除
    case DATA_SOURCE_DELETE.LOAD:
      return state.setIn(['dataSourceDelete','loading'],true)
    case DATA_SOURCE_DELETE.SUCCESS:
      return state
        .setIn(['dataSourceDelete', 'loading'], false)
        .setIn(['dataSourceDelete', 'loaded'], true)
        .setIn(['dataSourceDelete', 'result'], action.result)
    case DATA_SOURCE_DELETE.FAIL:
      return state
        .setIn(['dataSourceDelete', 'loading'], false)
        .setIn(['dataSourceDelete', 'loaded'], true)
        .setIn(['dataSourceDelete', 'result'], action.result)
    // dataSource 修改
    case DATA_SOURCE_UPDATE.LOAD:
      return state.setIn(['dataSourceUpdate','loading'],true)
    case DATA_SOURCE_UPDATE.SUCCESS:
      return state
        .setIn(['dataSourceUpdate', 'loading'], false)
        .setIn(['dataSourceUpdate', 'loaded'], true)
        .setIn(['dataSourceUpdate', 'result'], action.result)
    case DATA_SOURCE_UPDATE.FAIL:
      return state
        .setIn(['dataSourceUpdate', 'loading'], false)
        .setIn(['dataSourceUpdate', 'loaded'], true)
        .setIn(['dataSourceUpdate', 'result'], action.result)
    // dataSource 增加
    case DATA_SOURCE_INSERT.LOAD:
      return state.setIn(['dataSourceInsert','loading'],true)
    case DATA_SOURCE_INSERT.SUCCESS:
      return state
        .setIn(['dataSourceInsert', 'loading'], false)
        .setIn(['dataSourceInsert', 'loaded'], true)
        .setIn(['dataSourceInsert', 'result'], action.result)
    case DATA_SOURCE_INSERT.FAIL:
      return state
        .setIn(['dataSourceInsert', 'loading'], false)
        .setIn(['dataSourceInsert', 'loaded'], true)
        .setIn(['dataSourceInsert', 'result'], action.result)
    // topology kill
    case KILL_TOPOLOGY.LOAD:
      return state.setIn(['killTopologyResult','loading'],true)
    case KILL_TOPOLOGY.SUCCESS:
      return state
        .setIn(['killTopologyResult', 'loading'], false)
        .setIn(['killTopologyResult', 'loaded'], true)
        .setIn(['killTopologyResult', 'result'], action.result)
    case KILL_TOPOLOGY.FAIL:
      return state
        .setIn(['killTopologyResult', 'loading'], false)
        .setIn(['killTopologyResult', 'loaded'], true)
        .setIn(['killTopologyResult', 'result'], action.result)
    // 获取schema list by ds id
    case DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.LOAD:
      return state.setIn(['schemaList','loading'],true)
    case DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.SUCCESS:
      return state
        .setIn(['schemaList', 'loading'], false)
        .setIn(['schemaList', 'loaded'], true)
        .setIn(['schemaList', 'result'], action.result)
    case DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.FAIL:
      return state
        .setIn(['schemaList', 'loading'], false)
        .setIn(['schemaList', 'loaded'], true)
        .setIn(['schemaList', 'result'], action.result)
    // 获取schema 和 table
    case DATA_SOURCE_GET_SCHEMA_TABLE_LIST.LOAD:
      return state.setIn(['schemaTableResult','loading'],true)
    case DATA_SOURCE_GET_SCHEMA_TABLE_LIST.SUCCESS:
      return state
        .setIn(['schemaTableResult', 'loading'], false)
        .setIn(['schemaTableResult', 'loaded'], true)
        .setIn(['schemaTableResult', 'result'], action.result)
    case DATA_SOURCE_GET_SCHEMA_TABLE_LIST.FAIL:
      return state
        .setIn(['schemaTableResult', 'loading'], false)
        .setIn(['schemaTableResult', 'loaded'], true)
        .setIn(['schemaTableResult', 'result'], action.result)
    // dataSource 管理查询参数
    case DATA_SOURCE_ALL_PARAMS:
      return state.setIn(['dataSourceParams'], action.params)
    // DataSource 清除schema 和 schema table数据
    case DATA_SOURCE_CLEAN_SCHEMA_TABLE:
      return state.setIn(['schemaTableResult', 'result'], {})
    case OGG_CANAL_CONF_GET_BY_DS_NAME.LOAD:
      return state.setIn(['oggCanalConfDsName','loading'],true)
    case OGG_CANAL_CONF_GET_BY_DS_NAME.SUCCESS:
      return state
        .setIn(['oggCanalConfDsName', 'loading'], false)
        .setIn(['oggCanalConfDsName', 'loaded'], true)
        .setIn(['oggCanalConfDsName', 'result'], action.result)
    case OGG_CANAL_CONF_GET_BY_DS_NAME.FAIL:
      return state
        .setIn(['oggCanalConfDsName', 'loading'], false)
        .setIn(['oggCanalConfDsName', 'loaded'], true)
        .setIn(['oggCanalConfDsName', 'result'], action.result)
    default:
      return state
  }
}
