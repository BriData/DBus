/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  ALL_USER_LIST_PARAMS,
  ALL_USER_LIST_SEARCH,
  ALL_USER_INFO,
  ALL_USER_PROJECT_INFO
} from '../action/types'

const initialState = fromJS({
  userList: {
    loading: false,
    loaded: false,
    result: {}
  },
  userInfo: {
    loading: false,
    loaded: false,
    result: {}
  },
  userProject: {
    loading: false,
    loaded: false,
    result: {}
  },
  userListParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 用户列表查询
    case ALL_USER_LIST_SEARCH.LOAD:
      return state.setIn(['userList', 'loading'], true)
    case ALL_USER_LIST_SEARCH.SUCCESS:
      return state
        .setIn(['userList', 'loading'], false)
        .setIn(['userList', 'loaded'], true)
        .setIn(['userList', 'result'], action.result)
    case ALL_USER_LIST_SEARCH.FAIL:
      return state
        .setIn(['userList', 'loading'], false)
        .setIn(['userList', 'loaded'], true)
        .setIn(['userList', 'result'], action.result)
    // 获取用户信息
    case ALL_USER_INFO.LOAD:
      return state.setIn(['userInfo', 'loading'], true)
    case ALL_USER_INFO.SUCCESS:
      return state
        .setIn(['userInfo', 'loading'], false)
        .setIn(['userInfo', 'loaded'], true)
        .setIn(['userInfo', 'result'], action.result)
    case ALL_USER_INFO.FAIL:
      return state
        .setIn(['userInfo', 'loading'], false)
        .setIn(['userInfo', 'loaded'], true)
        .setIn(['userInfo', 'result'], action.result)
    // 获取项目信息
    case ALL_USER_PROJECT_INFO.LOAD:
      return state.setIn(['userProject', 'loading'], true)
    case ALL_USER_PROJECT_INFO.SUCCESS:
      return state
        .setIn(['userProject', 'loading'], false)
        .setIn(['userProject', 'loaded'], true)
        .setIn(['userProject', 'result'], action.result)
    case ALL_USER_PROJECT_INFO.FAIL:
      return state
        .setIn(['userProject', 'loading'], false)
        .setIn(['userProject', 'loaded'], true)
        .setIn(['userProject', 'result'], action.result)
    // 用户列表查询参数
    case ALL_USER_LIST_PARAMS:
      return state.setIn(['userListParams'], action.params)
    default:
      return state
  }
}
