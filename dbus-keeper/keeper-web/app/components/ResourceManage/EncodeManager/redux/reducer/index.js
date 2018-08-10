/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  ENCODE_MANAGER_SET_PARAM,
  ENCODE_MANAGER_SEARCH
} from '../action/types'

const initialState = fromJS({
  encodeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  params: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    case ENCODE_MANAGER_SEARCH.LOAD:
      return state.setIn(['encodeList', 'loading'], true)
    case ENCODE_MANAGER_SEARCH.SUCCESS:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)
    case ENCODE_MANAGER_SEARCH.FAIL:
      return state
        .setIn(['encodeList', 'loading'], false)
        .setIn(['encodeList', 'loaded'], true)
        .setIn(['encodeList', 'result'], action.result)

    case ENCODE_MANAGER_SET_PARAM:
      return state.setIn(['params'], action.params)
    default:
      return state
  }
}
