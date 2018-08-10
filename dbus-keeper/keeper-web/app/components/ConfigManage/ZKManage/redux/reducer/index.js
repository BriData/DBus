/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  ZK_MANAGE_LOAD_LEVEL_OF_PATH,
  READ_ZK_DATA,
  SAVE_ZK_DATA,
  READ_ZK_PROPERTIES,
  SAVE_ZK_PROPERTIES,
  LOAD_ZK_TREE_BY_DSNAME
} from '../action/types'

const initialState = fromJS({
  levelOfPath: {
    loading: false,
    loaded: false,
    result: {}
  },
  // zk的string返回值
  zkData: {
    loading: false,
    loaded: false,
    result: {}
  },
  saveResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  // zk的properties返回值
  zkProperties: {
    loading: false,
    loaded: false,
    result: {}
  },
  savePropertiesResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  dataSourceZkTree: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    // 查询节点树
    case ZK_MANAGE_LOAD_LEVEL_OF_PATH.LOAD:
      return state.setIn(['levelOfPath','loading'],true)
    case ZK_MANAGE_LOAD_LEVEL_OF_PATH.SUCCESS:
      return state
        .setIn(['levelOfPath', 'loading'], false)
        .setIn(['levelOfPath', 'loaded'], true)
        .setIn(['levelOfPath', 'result'], action.result)
    case ZK_MANAGE_LOAD_LEVEL_OF_PATH.FAIL:
      return state
        .setIn(['levelOfPath', 'loading'], false)
        .setIn(['levelOfPath', 'loaded'], true)
        .setIn(['levelOfPath', 'result'], action.result)
    // 读zk data
    case READ_ZK_DATA.LOAD:
      return state.setIn(['zkData','loading'],true)
    case READ_ZK_DATA.SUCCESS:
      return state
        .setIn(['zkData', 'loading'], false)
        .setIn(['zkData', 'loaded'], true)
        .setIn(['zkData', 'result'], action.result)
    case READ_ZK_DATA.FAIL:
      return state
        .setIn(['zkData', 'loading'], false)
        .setIn(['zkData', 'loaded'], true)
        .setIn(['zkData', 'result'], action.result)
    // 保存zk data
    case SAVE_ZK_DATA.LOAD:
      return state.setIn(['saveResult','loading'],true)
    case SAVE_ZK_DATA.SUCCESS:
      return state
        .setIn(['saveResult', 'loading'], false)
        .setIn(['saveResult', 'loaded'], true)
        .setIn(['saveResult', 'result'], action.result)
    case SAVE_ZK_DATA.FAIL:
      return state
        .setIn(['saveResult', 'loading'], false)
        .setIn(['saveResult', 'loaded'], true)
        .setIn(['saveResult', 'result'], action.result)
    // 读zk properties
    case READ_ZK_PROPERTIES.LOAD:
      return state.setIn(['zkProperties','loading'],true)
    case READ_ZK_PROPERTIES.SUCCESS:
      return state
        .setIn(['zkProperties', 'loading'], false)
        .setIn(['zkProperties', 'loaded'], true)
        .setIn(['zkProperties', 'result'], action.result)
    case READ_ZK_PROPERTIES.FAIL:
      return state
        .setIn(['zkProperties', 'loading'], false)
        .setIn(['zkProperties', 'loaded'], true)
        .setIn(['zkProperties', 'result'], action.result)
    // 保存zk properties
    case SAVE_ZK_PROPERTIES.LOAD:
      return state.setIn(['savePropertiesResult','loading'],true)
    case SAVE_ZK_PROPERTIES.SUCCESS:
      return state
        .setIn(['savePropertiesResult', 'loading'], false)
        .setIn(['savePropertiesResult', 'loaded'], true)
        .setIn(['savePropertiesResult', 'result'], action.result)
    case SAVE_ZK_PROPERTIES.FAIL:
      return state
        .setIn(['savePropertiesResult', 'loading'], false)
        .setIn(['savePropertiesResult', 'loaded'], true)
        .setIn(['savePropertiesResult', 'result'], action.result)
    // LOAD_ZK_TREE_BY_DSNAME
    case LOAD_ZK_TREE_BY_DSNAME.LOAD:
      return state.setIn(['dataSourceZkTree','loading'],true)
    case LOAD_ZK_TREE_BY_DSNAME.SUCCESS:
      return state
        .setIn(['dataSourceZkTree', 'loading'], false)
        .setIn(['dataSourceZkTree', 'loaded'], true)
        .setIn(['dataSourceZkTree', 'result'], action.result)
    case LOAD_ZK_TREE_BY_DSNAME.FAIL:
      return state
        .setIn(['dataSourceZkTree', 'loading'], false)
        .setIn(['dataSourceZkTree', 'loaded'], true)
        .setIn(['dataSourceZkTree', 'result'], action.result)
    default:
      return state
  }
}
