/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  SEARCH_FROM_SOURCE,
  CLEAR_SOURCE
} from '../action/types'

const initialState = fromJS({
  sourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 管理查询
    case SEARCH_FROM_SOURCE.LOAD:
      return state.setIn(['sourceList','loading'],true)
    case SEARCH_FROM_SOURCE.SUCCESS:
      return state
        .setIn(['sourceList', 'loading'], false)
        .setIn(['sourceList', 'loaded'], true)
        .setIn(['sourceList', 'result'], action.result)
    case SEARCH_FROM_SOURCE.FAIL:
      return state
        .setIn(['sourceList', 'loading'], false)
        .setIn(['sourceList', 'loaded'], true)
        .setIn(['sourceList', 'result'], action.result)
    case CLEAR_SOURCE:
      return state
        .setIn(['sourceList', 'loading'], false)
        .setIn(['sourceList', 'loaded'], true)
        .setIn(['sourceList', 'result'], {})
    default:
      return state
  }
}
