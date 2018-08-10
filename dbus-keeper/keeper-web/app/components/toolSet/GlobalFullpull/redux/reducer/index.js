/**
 * @author xiancangao
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  GLOBAL_FULL_PULL
} from '../action/types'

const initialState = fromJS({
  globalFullPullResult: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    case GLOBAL_FULL_PULL.LOAD:
      return state.setIn(['globalFullPullResult', 'loading'], true)
    case GLOBAL_FULL_PULL.SUCCESS:
      return state
        .setIn(['globalFullPullResult', 'loading'], false)
        .setIn(['globalFullPullResult', 'loaded'], true)
        .setIn(['globalFullPullResult', 'result'], action.result)
    case GLOBAL_FULL_PULL.FAIL:
      return state
        .setIn(['globalFullPullResult', 'loading'], false)
        .setIn(['globalFullPullResult', 'loaded'], true)
        .setIn(['globalFullPullResult', 'result'], action.result)

    default:
      return state
  }
}
