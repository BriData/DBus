/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  DBA_ENCODE_ALL_SEARCH
} from '../action/types'
import {
  DBA_ENCODE_SEARCH_PARAM
} from '../action/types'

const initialState = fromJS({
  dbaEncodeList: {
    loading: false,
    loaded: false,
    result: {}
  },
  dbaEncodeParams: {}
})

export default (state = initialState, action) => {
  switch (action.type) {
    case DBA_ENCODE_SEARCH_PARAM:
      return state.setIn(['dbaEncodeParams'], action.params)
    case DBA_ENCODE_ALL_SEARCH.LOAD:
      return state.setIn(['dbaEncodeList','loading'],true)
    case DBA_ENCODE_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['dbaEncodeList', 'loading'], false)
        .setIn(['dbaEncodeList', 'loaded'], true)
        .setIn(['dbaEncodeList', 'result'], action.result)
    case DBA_ENCODE_ALL_SEARCH.FAIL:
      return state
        .setIn(['dbaEncodeList', 'loading'], false)
        .setIn(['dbaEncodeList', 'loaded'], true)
        .setIn(['dbaEncodeList', 'result'], action.result)
    default:
      return state
  }
}
