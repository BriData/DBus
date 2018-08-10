/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DATA_SCHEMA_ALL_PARAMS,
  DATA_SCHEMA_ALL_SEARCH,
  DATA_SCHEMA_SEARCH_ALL_LIST,
  DATA_SCHEMA_GET_BY_ID,
} from '../action/types'

const initialState = fromJS({
  dataSchemaList: {
    loading: false,
    loaded: false,
    result: {}
  },
  allDataSchemaList: {
    loading: false,
    loaded: false,
    result: {}
  },
  theDataSchemaGottenById: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSchemaParams: null,
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 管理查询
    case DATA_SCHEMA_ALL_SEARCH.LOAD:
      return state.setIn(['dataSchemaList','loading'],true)
    case DATA_SCHEMA_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['dataSchemaList', 'loading'], false)
        .setIn(['dataSchemaList', 'loaded'], true)
        .setIn(['dataSchemaList', 'result'], action.result)
    case DATA_SCHEMA_ALL_SEARCH.FAIL:
      return state
        .setIn(['dataSchemaList', 'loading'], false)
        .setIn(['dataSchemaList', 'loaded'], true)
        .setIn(['dataSchemaList', 'result'], action.result)
    // 管理查询
    case DATA_SCHEMA_GET_BY_ID.LOAD:
      return state.setIn(['theDataSchemaGottenById','loading'],true)
    case DATA_SCHEMA_GET_BY_ID.SUCCESS:
      return state
        .setIn(['theDataSchemaGottenById', 'loading'], false)
        .setIn(['theDataSchemaGottenById', 'loaded'], true)
        .setIn(['theDataSchemaGottenById', 'result'], action.result)
    case DATA_SCHEMA_GET_BY_ID.FAIL:
      return state
        .setIn(['theDataSchemaGottenById', 'loading'], false)
        .setIn(['theDataSchemaGottenById', 'loaded'], true)
        .setIn(['theDataSchemaGottenById', 'result'], action.result)
    // DataSchema搜索所有信息，不需要分页
    case DATA_SCHEMA_SEARCH_ALL_LIST.LOAD:
      return state.setIn(['allDataSchemaList','loading'],true)
    case DATA_SCHEMA_SEARCH_ALL_LIST.SUCCESS:
      return state
        .setIn(['allDataSchemaList', 'loading'], false)
        .setIn(['allDataSchemaList', 'loaded'], true)
        .setIn(['allDataSchemaList', 'result'], action.result)
    case DATA_SCHEMA_SEARCH_ALL_LIST.FAIL:
      return state
        .setIn(['allDataSchemaList', 'loading'], false)
        .setIn(['allDataSchemaList', 'loaded'], true)
        .setIn(['allDataSchemaList', 'result'], action.result)
    // 管理查询参数
    case DATA_SCHEMA_ALL_PARAMS:
      return state.setIn(['dataSchemaParams'], action.params)
    default:
      return state
  }
}
