/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  SEARCH_RULE_GROUP
} from '../action/types'

const initialState = fromJS({
  ruleGroups: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 用户管理查询
    case SEARCH_RULE_GROUP.LOAD:
      return state.setIn(['ruleGroups', 'loading'], true)
    case SEARCH_RULE_GROUP.SUCCESS:
      return state
        .setIn(['ruleGroups', 'loading'], false)
        .setIn(['ruleGroups', 'loaded'], true)
        .setIn(['ruleGroups', 'result'], action.result)
    case SEARCH_RULE_GROUP.FAIL:
      return state
        .setIn(['ruleGroups', 'loading'], false)
        .setIn(['ruleGroups', 'loaded'], true)
        .setIn(['ruleGroups', 'result'], action.result)
    default:
      return state
  }
}
