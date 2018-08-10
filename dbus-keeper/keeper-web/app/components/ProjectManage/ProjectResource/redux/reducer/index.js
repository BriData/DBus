/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  PROJECT_RESOURCE_ALL_PARAMS,
  PROJECT_RESOURCE_ALL_SEARCH,
  PROJECT_RESOURCE_PROJECT_LIST_SEARCH,
  PROJECT_RESOURCE_DSNAME_LIST_SEARCH,
  PROJECT_RESOURCE_TABLE_ENCODE_SEARCH
} from '../action/types'

const initialState = fromJS({
  resourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  projectList: {
    loading: false,
    loaded: false,
    result: {}
  },
  dsNameList: {
    loading: false,
    loaded: false,
    result: {}
  },
  encodeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  resourceParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // Resource管理查询
    case PROJECT_RESOURCE_ALL_SEARCH.LOAD:
      return state.setIn(['resourceList', 'loading'], true)
    case PROJECT_RESOURCE_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)
    case PROJECT_RESOURCE_ALL_SEARCH.FAIL:
      return state
        .setIn(['resourceList', 'loading'], false)
        .setIn(['resourceList', 'loaded'], true)
        .setIn(['resourceList', 'result'], action.result)

    // Resource project查询
    case PROJECT_RESOURCE_PROJECT_LIST_SEARCH.LOAD:
      return state.setIn(['projectList', 'loading'], true)
    case PROJECT_RESOURCE_PROJECT_LIST_SEARCH.SUCCESS:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    case PROJECT_RESOURCE_PROJECT_LIST_SEARCH.FAIL:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)


    // Resource dsName查询
    case PROJECT_RESOURCE_DSNAME_LIST_SEARCH.LOAD:
      return state.setIn(['dsNameList', 'loading'], true)
    case PROJECT_RESOURCE_DSNAME_LIST_SEARCH.SUCCESS:
      return state
        .setIn(['dsNameList', 'loading'], false)
        .setIn(['dsNameList', 'loaded'], true)
        .setIn(['dsNameList', 'result'], action.result)
    case PROJECT_RESOURCE_DSNAME_LIST_SEARCH.FAIL:
      return state
        .setIn(['dsNameList', 'loading'], false)
        .setIn(['dsNameList', 'loaded'], true)
        .setIn(['dsNameList', 'result'], action.result)

    // Resource 脱敏查询
    case PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.LOAD:
      return state.setIn(['encodeList', 'loading'], true)
    case PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.SUCCESS:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)
    case PROJECT_RESOURCE_TABLE_ENCODE_SEARCH.FAIL:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)


    // Resource管理查询参数
    case PROJECT_RESOURCE_ALL_PARAMS:
      return state.setIn(['resourceParams'], action.params)
    default:
      return state
  }
}
