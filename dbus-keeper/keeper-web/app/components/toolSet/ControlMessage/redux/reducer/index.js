/**
 * @author xiancangao
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DATA_SOURCE_LIST,
  SEND_CONTROL_MESSAGE,
  READ_RELOAD_INFO
} from '../action/types'

const initialState = fromJS({
  dataSourceList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sendControlMessage: {
    loading: false,
    loaded: false,
    result: {}
  },
  reloadInfo: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    case DATA_SOURCE_LIST.LOAD:
      return state.setIn(['dataSourceList', 'loading'], true)
    case DATA_SOURCE_LIST.SUCCESS:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    case DATA_SOURCE_LIST.FAIL:
      return state
        .setIn(['dataSourceList', 'loading'], false)
        .setIn(['dataSourceList', 'loaded'], true)
        .setIn(['dataSourceList', 'result'], action.result)
    case SEND_CONTROL_MESSAGE.LOAD:
      return state.setIn(['sendControlMessage', 'loading'], true)
    case SEND_CONTROL_MESSAGE.SUCCESS:
      return state
        .setIn(['sendControlMessage', 'loading'], false)
        .setIn(['sendControlMessage', 'loaded'], true)
        .setIn(['sendControlMessage', 'result'], action.result)
    case SEND_CONTROL_MESSAGE.FAIL:
      return state
        .setIn(['sendControlMessage', 'loading'], false)
        .setIn(['sendControlMessage', 'loaded'], true)
        .setIn(['sendControlMessage', 'result'], action.result)
    case READ_RELOAD_INFO.LOAD:
      return state.setIn(['reloadInfo', 'loading'], true)
    case READ_RELOAD_INFO.SUCCESS:
      return state
        .setIn(['reloadInfo', 'loading'], false)
        .setIn(['reloadInfo', 'loaded'], true)
        .setIn(['reloadInfo', 'result'], action.result)
    case READ_RELOAD_INFO.FAIL:
      return state
        .setIn(['reloadInfo', 'loading'], false)
        .setIn(['reloadInfo', 'loaded'], true)
        .setIn(['reloadInfo', 'result'], action.result)

    default:
      return state
  }
}
