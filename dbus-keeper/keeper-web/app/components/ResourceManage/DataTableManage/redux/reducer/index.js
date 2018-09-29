/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DATA_TABLE_ALL_PARAMS,
  DATA_TABLE_ALL_SEARCH,
  DATA_TABLE_FIND_ALL_SEARCH,
  DATA_TABLE_GET_ENCODE_CONFIG,
  DATA_TABLE_GET_TABLE_COLUMN,
  DATA_TABLE_GET_ENCODE_TYPE,
  DATA_TABLE_GET_VERSION_LIST,
  DATA_TABLE_GET_VERSION_DETAIL,
  DATA_TABLE_CLEAR_VERSION_DETAIL,
  DATA_TABLE_SOURCE_INSIGHT
} from '../action/types'

const initialState = fromJS({
  jarInfos: {
    loading: false,
    loaded: false,
    result: {}
  },
  jarParams: null,
  dataTableList: {
    loading: false,
    loaded: false,
    result: {}
  },
  allDataTableList: {
    loading: false,
    loaded: false,
    result: {}
  },
  encodeConfigList: {
    loading: false,
    loaded: false,
    result: {}
  },
  tableColumnList: {
    loading: false,
    loaded: false,
    result: {}
  },
  encodeTypeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  versionList: {
    loading: false,
    loaded: false,
    result: {}
  },
  versionDetail: {
    loading: false,
    loaded: false,
    result: {}
  },
  sourceInsightResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataTableParams: null,
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 管理查询
    case DATA_TABLE_ALL_SEARCH.LOAD:
      return state.setIn(['dataTableList','loading'],true)
    case DATA_TABLE_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['dataTableList', 'loading'], false)
        .setIn(['dataTableList', 'loaded'], true)
        .setIn(['dataTableList', 'result'], action.result)
    case DATA_TABLE_ALL_SEARCH.FAIL:
      return state
        .setIn(['dataTableList', 'loading'], false)
        .setIn(['dataTableList', 'loaded'], true)
        .setIn(['dataTableList', 'result'], action.result)
    case DATA_TABLE_FIND_ALL_SEARCH.LOAD:
      return state.setIn(['allDataTableList','loading'],true)
    case DATA_TABLE_FIND_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['allDataTableList', 'loading'], false)
        .setIn(['allDataTableList', 'loaded'], true)
        .setIn(['allDataTableList', 'result'], action.result)
    case DATA_TABLE_FIND_ALL_SEARCH.FAIL:
      return state
        .setIn(['allDataTableList', 'loading'], false)
        .setIn(['allDataTableList', 'loaded'], true)
        .setIn(['allDataTableList', 'result'], action.result)
    // 管理查询参数
    case DATA_TABLE_ALL_PARAMS:
      return state.setIn(['dataTableParams'], action.params)
    case DATA_TABLE_CLEAR_VERSION_DETAIL:
      return state.setIn(['versionDetail','result'], {})
        .setIn(['versionList','result'], {})
    // 查询该表脱敏配置
    case DATA_TABLE_GET_ENCODE_CONFIG.LOAD:
      return state.setIn(['encodeConfigList', 'loading'], true)
    case DATA_TABLE_GET_ENCODE_CONFIG.SUCCESS:
      return state
        .setIn(['encodeConfigList', 'loading'], false)
        .setIn(['encodeConfigList', 'loaded'], true)
        .setIn(['encodeConfigList', 'result'], action.result)
    case DATA_TABLE_GET_ENCODE_CONFIG.FAIL:
      return state
        .setIn(['encodeConfigList', 'loading'], false)
        .setIn(['encodeConfigList', 'loaded'], true)
        .setIn(['encodeConfigList', 'result'], action.result)
    // 查询该表列meta
    case DATA_TABLE_GET_TABLE_COLUMN.LOAD:
      return state.setIn(['tableColumnList', 'loading'], true)
    case DATA_TABLE_GET_TABLE_COLUMN.SUCCESS:
      return state
        .setIn(['tableColumnList', 'loading'], false)
        .setIn(['tableColumnList', 'loaded'], true)
        .setIn(['tableColumnList', 'result'], action.result)
    case DATA_TABLE_GET_TABLE_COLUMN.FAIL:
      return state
        .setIn(['tableColumnList', 'loading'], false)
        .setIn(['tableColumnList', 'loaded'], true)
        .setIn(['tableColumnList', 'result'], action.result)
    // 查询脱敏类型列表
    case DATA_TABLE_GET_ENCODE_TYPE.LOAD:
      return state.setIn(['encodeTypeList', 'loading'], true)
    case DATA_TABLE_GET_ENCODE_TYPE.SUCCESS:
      return state
        .setIn(['encodeTypeList', 'loading'], false)
        .setIn(['encodeTypeList', 'loaded'], true)
        .setIn(['encodeTypeList', 'result'], action.result)
    case DATA_TABLE_GET_ENCODE_TYPE.FAIL:
      return state
        .setIn(['encodeTypeList', 'loading'], false)
        .setIn(['encodeTypeList', 'loaded'], true)
        .setIn(['encodeTypeList', 'result'], action.result)
    // 版本列表
    case DATA_TABLE_GET_VERSION_LIST.LOAD:
      return state.setIn(['versionList', 'loading'], true)
    case DATA_TABLE_GET_VERSION_LIST.SUCCESS:
      return state
        .setIn(['versionList', 'loading'], false)
        .setIn(['versionList', 'loaded'], true)
        .setIn(['versionList', 'result'], action.result)
    case DATA_TABLE_GET_VERSION_LIST.FAIL:
      return state
        .setIn(['versionList', 'loading'], false)
        .setIn(['versionList', 'loaded'], true)
        .setIn(['versionList', 'result'], action.result)
    // 版本具体信息
    case DATA_TABLE_GET_VERSION_DETAIL.LOAD:
      return state.setIn(['versionDetail', 'loading'], true)
    case DATA_TABLE_GET_VERSION_DETAIL.SUCCESS:
      return state
        .setIn(['versionDetail', 'loading'], false)
        .setIn(['versionDetail', 'loaded'], true)
        .setIn(['versionDetail', 'result'], action.result)
    case DATA_TABLE_GET_VERSION_DETAIL.FAIL:
      return state
        .setIn(['versionDetail', 'loading'], false)
        .setIn(['versionDetail', 'loaded'], true)
        .setIn(['versionDetail', 'result'], action.result)
    // 源端查看表列信息
    case DATA_TABLE_SOURCE_INSIGHT.LOAD:
      return state.setIn(['sourceInsightResult', 'loading'], true)
    case DATA_TABLE_SOURCE_INSIGHT.SUCCESS:
      return state
        .setIn(['sourceInsightResult', 'loading'], false)
        .setIn(['sourceInsightResult', 'loaded'], true)
        .setIn(['sourceInsightResult', 'result'], action.result)
    case DATA_TABLE_SOURCE_INSIGHT.FAIL:
      return state
        .setIn(['sourceInsightResult', 'loading'], false)
        .setIn(['sourceInsightResult', 'loaded'], true)
        .setIn(['sourceInsightResult', 'result'], action.result)
    default:
      return state
  }
}
