/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  PROJECT_GET_LIST,
  DELETE_PROJECT,
  ENABLE_DISABLE_PROJECT
} from '../action/types'

const initialState = fromJS({
  projectList: {
    loading: false,
    loaded: false,
    result: {}
  },
  deleteProject: {
    loading: false,
    loaded: false,
    result: {}
  },
  enableDisableProject: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 启用禁用项目
    case ENABLE_DISABLE_PROJECT.LOAD:
      return state.setIn(['enableDisableProject', 'loading'], true)
    case ENABLE_DISABLE_PROJECT.SUCCESS:
      return state
        .setIn(['enableDisableProject', 'loading'], false)
        .setIn(['enableDisableProject', 'loaded'], true)
        .setIn(['enableDisableProject', 'result'], action.result)
    case ENABLE_DISABLE_PROJECT.FAIL:
      return state
        .setIn(['enableDisableProject', 'loading'], false)
        .setIn(['enableDisableProject', 'loaded'], true)
        .setIn(['enableDisableProject', 'result'], action.result)
    // 删除项目
    case DELETE_PROJECT.LOAD:
      return state.setIn(['deleteProject', 'loading'], true)
    case DELETE_PROJECT.SUCCESS:
      return state
        .setIn(['deleteProject', 'loading'], false)
        .setIn(['deleteProject', 'loaded'], true)
        .setIn(['deleteProject', 'result'], action.result)
    case DELETE_PROJECT.FAIL:
      return state
        .setIn(['deleteProject', 'loading'], false)
        .setIn(['deleteProject', 'loaded'], true)
        .setIn(['deleteProject', 'result'], action.result)
    // 获取项目列表
    case PROJECT_GET_LIST.LOAD:
      return state.setIn(['projectList', 'loading'], true)
    case PROJECT_GET_LIST.SUCCESS:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    case PROJECT_GET_LIST.FAIL:
      return state
        .setIn(['projectList', 'loading'], false)
        .setIn(['projectList', 'loaded'], true)
        .setIn(['projectList', 'result'], action.result)
    default:
      return state
  }
}
