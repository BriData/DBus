/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  GET_BASIC_CONF
} from '../action/types'

const initialState = fromJS({
  basicConf: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 模拟请求
    case GET_BASIC_CONF.LOAD:
      return state.setIn(['basicConf', 'loading'], true)
    case GET_BASIC_CONF.SUCCESS:
      return state
        .setIn(['basicConf', 'loading'], false)
        .setIn(['basicConf', 'loaded'], true)
        .setIn(['basicConf', 'result'], action.result)
    case GET_BASIC_CONF.FAIL:
      return state
        .setIn(['basicConf', 'loading'], false)
        .setIn(['basicConf', 'loaded'], true)
        .setIn(['basicConf', 'result'], action.result)
    default:
      return state
  }
}
