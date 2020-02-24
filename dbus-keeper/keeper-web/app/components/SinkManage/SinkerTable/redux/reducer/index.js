/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import {fromJS} from 'immutable'

// 导入types
import {
  SEARCH_SINKER_TABLE_LIST,
  SET_SEARCH_SINKER_TABLE_PARAM
} from '../action/types'

const initialState = fromJS({
  sinkerTableList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sinkerTableParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    case SEARCH_SINKER_TABLE_LIST.LOAD:
      return state.setIn(['sinkerTableList', 'loading'], true)
    case SEARCH_SINKER_TABLE_LIST.SUCCESS:
      return state
        .setIn(['sinkerTableList', 'loading'], false)
        .setIn(['sinkerTableList', 'loaded'], true)
        .setIn(['sinkerTableList', 'result'], action.result)
    case SEARCH_SINKER_TABLE_LIST.FAIL:
      return state
        .setIn(['sinkerTableList', 'loading'], false)
        .setIn(['sinkerTableList', 'loaded'], true)
        .setIn(['sinkerTableList', 'result'], action.result)
    case SET_SEARCH_SINKER_TABLE_PARAM:
      return state.setIn(['sinkerTableParams'], action.params)
    default:
      return state
  }
}
