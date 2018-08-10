/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  SEARCH_JAR_INFOS
} from '../action/types'

const initialState = fromJS({
  jarInfos: {
    loading: false,
    loaded: false,
    result: {}
  },
  jarParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 用户管理查询
    case SEARCH_JAR_INFOS.LOAD:
      return state.setIn(['jarInfos', 'loading'], true)
    case SEARCH_JAR_INFOS.SUCCESS:
      return state
        .setIn(['jarInfos', 'loading'], false)
        .setIn(['jarInfos', 'loaded'], true)
        .setIn(['jarInfos', 'result'], action.result)
    case SEARCH_JAR_INFOS.FAIL:
      return state
        .setIn(['jarInfos', 'loading'], false)
        .setIn(['jarInfos', 'loaded'], true)
        .setIn(['jarInfos', 'result'], action.result)
    default:
      return state
  }
}
