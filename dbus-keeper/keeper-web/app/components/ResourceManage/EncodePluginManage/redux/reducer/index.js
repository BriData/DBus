/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  SET_ENCODE_PLUGIN_PARAM,
  SEARCH_ENCODE_PLUGIN
} from '../action/types'

const initialState = fromJS({
  encodePlugins: {
    loading: false,
    loaded: false,
    result: {}
  },
  encodePluginParam: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 用户管理查询
    case SEARCH_ENCODE_PLUGIN.LOAD:
      return state.setIn(['encodePlugins', 'loading'], true)
    case SEARCH_ENCODE_PLUGIN.SUCCESS:
      return state
        .setIn(['encodePlugins', 'loading'], false)
        .setIn(['encodePlugins', 'loaded'], true)
        .setIn(['encodePlugins', 'result'], action.result)
    case SEARCH_ENCODE_PLUGIN.FAIL:
      return state
        .setIn(['encodePlugins', 'loading'], false)
        .setIn(['encodePlugins', 'loaded'], true)
        .setIn(['encodePlugins', 'result'], action.result)
    case SET_ENCODE_PLUGIN_PARAM:
      return state.setIn(['encodePluginParam'], action.params)
    default:
      return state
  }
}
