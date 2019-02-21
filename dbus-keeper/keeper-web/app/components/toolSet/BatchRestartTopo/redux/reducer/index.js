/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DATA_SOURCE_ALL_PARAMS,
  DATA_SOURCE_ALL_SEARCH,
  DATA_SOURCE_GET_ID_TYPE_NAME
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
  killTopologyResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceParams: null,
  jarParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
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
    // dataSource 管理查询参数
    case DATA_SOURCE_ALL_PARAMS:
      return state.setIn(['dataSourceParams'], action.params)
    // DataSource 清除schema 和 schema table数据
    default:
      return state
  }
}
