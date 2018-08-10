/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  SEARCH_SINK_LIST,
  SET_SEARCH_SINK_PARAM
} from '../action/types'

const initialState = fromJS({
  sinkList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sinkParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // Sink管理查询
    case SEARCH_SINK_LIST.LOAD:
      return state.setIn(['sinkList', 'loading'], true)
    case SEARCH_SINK_LIST.SUCCESS:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    case SEARCH_SINK_LIST.FAIL:
      return state
        .setIn(['sinkList', 'loading'], false)
        .setIn(['sinkList', 'loaded'], true)
        .setIn(['sinkList', 'result'], action.result)
    // 设置Sink搜索
    case SET_SEARCH_SINK_PARAM:
      return state.setIn(['sinkParams'], action.params)
    default:
      return state
  }
}
