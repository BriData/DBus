/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import {fromJS} from 'immutable'

// 导入types
import {
  SEARCH_SINKER_SCHEMA_LIST,
  SET_SEARCH_SINKER_SCHEMA_PARAM
} from '../action/types'

const initialState = fromJS({
  sinkerSchemaList: {
    loading: false,
    loaded: false,
    result: {}
  },
  sinkerSchemaParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    case SEARCH_SINKER_SCHEMA_LIST.LOAD:
      return state.setIn(['sinkerSchemaList', 'loading'], true)
    case SEARCH_SINKER_SCHEMA_LIST.SUCCESS:
      return state
        .setIn(['sinkerSchemaList', 'loading'], false)
        .setIn(['sinkerSchemaList', 'loaded'], true)
        .setIn(['sinkerSchemaList', 'result'], action.result)
    case SEARCH_SINKER_SCHEMA_LIST.FAIL:
      return state
        .setIn(['sinkerSchemaList', 'loading'], false)
        .setIn(['sinkerSchemaList', 'loaded'], true)
        .setIn(['sinkerSchemaList', 'result'], action.result)
    case SET_SEARCH_SINKER_SCHEMA_PARAM:
      return state.setIn(['sinkerSchemaParams'], action.params)
    default:
      return state
  }
}
