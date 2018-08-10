/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  PROJECT_FULLPULL_HISTORY_ALL_PARAMS,
  PROJECT_FULLPULL_HISTORY_ALL_SEARCH,
  PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH,
  PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH
} from '../action/types'

const initialState = fromJS({
  fullpullList: {
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
  fullpullParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // fullpullHistory管理查询
    case PROJECT_FULLPULL_HISTORY_ALL_SEARCH.LOAD:
      return state.setIn(['fullpullList', 'loading'], true)
    case PROJECT_FULLPULL_HISTORY_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['fullpullList', 'loading'], false)
        .setIn(['fullpullList', 'loaded'], true)
        .setIn(['fullpullList', 'result'], action.result)
    case PROJECT_FULLPULL_HISTORY_ALL_SEARCH.FAIL:
      return state
        .setIn(['fullpullList', 'loading'], false)
        .setIn(['fullpullList', 'loaded'], true)
        .setIn(['fullpullList', 'result'], action.result)

    // fullpullHistory project查询
    case PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.LOAD:
      return state.setIn(['projectList', 'loading'], true)
    case PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.SUCCESS:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    case PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.FAIL:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)

    // FULLPULL_HISTORY dsName查询
    case PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.LOAD:
      return state.setIn(['dsNameList', 'loading'], true)
    case PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.SUCCESS:
      return state
        .setIn(['dsNameList', 'loading'], false)
        .setIn(['dsNameList', 'loaded'], true)
        .setIn(['dsNameList', 'result'], action.result)
    case PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.FAIL:
      return state
        .setIn(['dsNameList', 'loading'], false)
        .setIn(['dsNameList', 'loaded'], true)
        .setIn(['dsNameList', 'result'], action.result)

    // fullpullHistory管理查询参数
    case PROJECT_FULLPULL_HISTORY_ALL_PARAMS:
      return state.setIn(['fullpullParams'], action.params)
    default:
      return state
  }
}
