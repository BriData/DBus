/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  LATEST_JAR_GET_PATH
} from '../action/types'

const initialState = fromJS({
  jarPath: {
    loading: false,
    loaded: false,
    result: {}
  },
})

export default (state = initialState, action) => {
  switch (action.type) {
    case LATEST_JAR_GET_PATH.LOAD:
      return state.setIn(['jarPath', 'loading'], true)
    case LATEST_JAR_GET_PATH.SUCCESS:
      return state
        .setIn(['jarPath', 'loading'], false)
        .setIn(['jarPath', 'loaded'], true)
        .setIn(['jarPath', 'result'], action.result)
    case LATEST_JAR_GET_PATH.FAIL:
      return state
        .setIn(['jarPath', 'loading'], false)
        .setIn(['jarPath', 'loaded'], true)
        .setIn(['jarPath', 'result'], action.result)
    default:
      return state
  }
}
