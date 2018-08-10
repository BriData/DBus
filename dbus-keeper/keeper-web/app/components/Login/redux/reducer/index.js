/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  LOGIN_SAVE_PARAMS,
  LOGIN_GET_LIST
} from '../action/types'

const initialState = fromJS({
  list: {
    loading: false,
    loaded: false,
    result: {}
  },
  params: {}
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 模拟请求
    case LOGIN_GET_LIST.LOAD:
      return state.setIn(['list', 'loading'], true)
    case LOGIN_GET_LIST.SUCCESS:
      return state
        .setIn(['list', 'loading'], false)
        .setIn(['list', 'loaded'], true)
        .setIn(['list', 'result'], action.result)
    case LOGIN_GET_LIST.FAIL:
      return state
        .setIn(['list', 'loading'], false)
        .setIn(['list', 'loaded'], true)
        .setIn(['list', 'result'], action.result)
    // 模拟本地存储
    case LOGIN_SAVE_PARAMS:
      return state.setIn(['params'], action.params)
    default:
      return state
  }
}
