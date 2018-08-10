/**
 * @author 戎晓伟
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  PROJECT_TOPOLOGY_ALL_PARAMS,
  PROJECT_TOPOLOGY_ALL_SEARCH,
  PROJECT_TOPOLOGY_INFO,
  PROJECT_TOPOLOGY_START_INFO_SEARCH,
  PROJECT_TOPOLOGY_STOP_INFO_SEARCH,
  PROJECT_TOPOLOGY_FEEDS_SEARCH,
  PROJECT_TOPOLOGY_OUTTOPIC_SEARCH,
  PROJECT_TOPOLOGY_JAR_VERSIONS,
  PROJECT_TOPOLOGY_JAR_PACKAGES,
  PROJECT_TOPOLOGY_EFFECT,
  PROJECT_TOPOLOGY_RERUN_INIT
} from '../action/types'

const initialState = fromJS({
  topologyList: {
    loading: false,
    loaded: false,
    result: {}
  },
  topoInfo: {
    loading: false,
    loaded: false,
    result: {}
  },
  startInfo: {
    loading: false,
    loaded: false,
    result: {}
  },
  stopInfo: {
    loading: false,
    loaded: false,
    result: {}
  },
  feeds: {
    loading: false,
    loaded: false,
    result: {}
  },
  outTopic: {
    loading: false,
    loaded: false,
    result: {}
  },
  jarVersions: {
    loading: false,
    loaded: false,
    result: {}
  },
  jarPackages: {
    loading: false,
    loaded: false,
    result: {}
  },
  effectResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  rerunInitResult: {
    loading: false,
    loaded: false,
    result: {}
  },
  topologyParams: null
})

export default (state = initialState, action) => {
  switch (action.type) {
    // topology管理查询
    case PROJECT_TOPOLOGY_ALL_SEARCH.LOAD:
      return state.setIn(['topologyList', 'loading'], true)
    case PROJECT_TOPOLOGY_ALL_SEARCH.SUCCESS:
      return state
        .setIn(['topologyList', 'loading'], false)
        .setIn(['topologyList', 'loaded'], true)
        .setIn(['topologyList', 'result'], action.result)
    case PROJECT_TOPOLOGY_ALL_SEARCH.FAIL:
      return state
        .setIn(['topologyList', 'loading'], false)
        .setIn(['topologyList', 'loaded'], true)
        .setIn(['topologyList', 'result'], action.result)
    // Topology 获取某项 Topology 信息
    case PROJECT_TOPOLOGY_INFO.LOAD:
      return state.setIn(['topoInfo', 'loading'], true)
    case PROJECT_TOPOLOGY_INFO.SUCCESS:
      return state
        .setIn(['topoInfo', 'loading'], false)
        .setIn(['topoInfo', 'loaded'], true)
        .setIn(['topoInfo', 'result'], action.result)
    case PROJECT_TOPOLOGY_INFO.FAIL:
      return state
        .setIn(['topoInfo', 'loading'], false)
        .setIn(['topoInfo', 'loaded'], true)
        .setIn(['topoInfo', 'result'], action.result)
    // Topology 获取开始信息
    case PROJECT_TOPOLOGY_START_INFO_SEARCH.LOAD:
      return state.setIn(['startInfo', 'loading'], true)
    case PROJECT_TOPOLOGY_START_INFO_SEARCH.SUCCESS:
      return state
        .setIn(['startInfo', 'loading'], false)
        .setIn(['startInfo', 'loaded'], true)
        .setIn(['startInfo', 'result'], action.result)
    case PROJECT_TOPOLOGY_START_INFO_SEARCH.FAIL:
      return state
        .setIn(['startInfo', 'loading'], false)
        .setIn(['startInfo', 'loaded'], true)
        .setIn(['startInfo', 'result'], action.result)
    // Topology 获取停止日志
    case PROJECT_TOPOLOGY_STOP_INFO_SEARCH.LOAD:
      return state.setIn(['stopInfo', 'loading'], true)
    case PROJECT_TOPOLOGY_STOP_INFO_SEARCH.SUCCESS:
      return state
        .setIn(['stopInfo', 'loading'], false)
        .setIn(['stopInfo', 'loaded'], true)
        .setIn(['stopInfo', 'result'], action.result)
    case PROJECT_TOPOLOGY_STOP_INFO_SEARCH.FAIL:
      return state
        .setIn(['stopInfo', 'loading'], false)
        .setIn(['stopInfo', 'loaded'], true)
        .setIn(['stopInfo', 'result'], action.result)
    // Topology 获取订阅源
    case PROJECT_TOPOLOGY_FEEDS_SEARCH.LOAD:
      return state.setIn(['feeds', 'loading'], true)
    case PROJECT_TOPOLOGY_FEEDS_SEARCH.SUCCESS:
      return state
        .setIn(['feeds', 'loading'], false)
        .setIn(['feeds', 'loaded'], true)
        .setIn(['feeds', 'result'], action.result)
    case PROJECT_TOPOLOGY_FEEDS_SEARCH.FAIL:
      return state
        .setIn(['feeds', 'loading'], false)
        .setIn(['feeds', 'loaded'], true)
        .setIn(['feeds', 'result'], action.result)
    // Topology 获取输出topic列表
    case PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.LOAD:
      return state.setIn(['outTopic', 'loading'], true)
    case PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.SUCCESS:
      return state
        .setIn(['outTopic', 'loading'], false)
        .setIn(['outTopic', 'loaded'], true)
        .setIn(['outTopic', 'result'], action.result)
    case PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.FAIL:
      return state
        .setIn(['outTopic', 'loading'], false)
        .setIn(['outTopic', 'loaded'], true)
        .setIn(['outTopic', 'result'], action.result)
    // Topology 获取Jar版本
    case PROJECT_TOPOLOGY_JAR_VERSIONS.LOAD:
      return state.setIn(['jarVersions', 'loading'], true)
    case PROJECT_TOPOLOGY_JAR_VERSIONS.SUCCESS:
      return state
        .setIn(['jarVersions', 'loading'], false)
        .setIn(['jarVersions', 'loaded'], true)
        .setIn(['jarVersions', 'result'], action.result)
    case PROJECT_TOPOLOGY_JAR_VERSIONS.FAIL:
      return state
        .setIn(['jarVersions', 'loading'], false)
        .setIn(['jarVersions', 'loaded'], true)
        .setIn(['jarVersions', 'result'], action.result)
    // Topology 获取Jar包
    case PROJECT_TOPOLOGY_JAR_PACKAGES.LOAD:
      return state.setIn(['jarPackages', 'loading'], true)
    case PROJECT_TOPOLOGY_JAR_PACKAGES.SUCCESS:
      return state
        .setIn(['jarPackages', 'loading'], false)
        .setIn(['jarPackages', 'loaded'], true)
        .setIn(['jarPackages', 'result'], action.result)
    case PROJECT_TOPOLOGY_JAR_PACKAGES.FAIL:
      return state
        .setIn(['jarPackages', 'loading'], false)
        .setIn(['jarPackages', 'loaded'], true)
        .setIn(['jarPackages', 'result'], action.result)
    // Topology 生效
    case PROJECT_TOPOLOGY_EFFECT.LOAD:
      return state.setIn(['effectResult', 'loading'], true)
    case PROJECT_TOPOLOGY_EFFECT.SUCCESS:
      return state
        .setIn(['effectResult', 'loading'], false)
        .setIn(['effectResult', 'loaded'], true)
        .setIn(['effectResult', 'result'], action.result)
    case PROJECT_TOPOLOGY_EFFECT.FAIL:
      return state
        .setIn(['effectResult', 'loading'], false)
        .setIn(['effectResult', 'loaded'], true)
        .setIn(['effectResult', 'result'], action.result)
    // Topology rerun init
    case PROJECT_TOPOLOGY_RERUN_INIT.LOAD:
      return state.setIn(['rerunInitResult', 'loading'], true)
    case PROJECT_TOPOLOGY_RERUN_INIT.SUCCESS:
      return state
        .setIn(['rerunInitResult', 'loading'], false)
        .setIn(['rerunInitResult', 'loaded'], true)
        .setIn(['rerunInitResult', 'result'], action.result)
    case PROJECT_TOPOLOGY_RERUN_INIT.FAIL:
      return state
        .setIn(['rerunInitResult', 'loading'], false)
        .setIn(['rerunInitResult', 'loaded'], true)
        .setIn(['rerunInitResult', 'result'], action.result)
    // topology管理查询参数
    case PROJECT_TOPOLOGY_ALL_PARAMS:
      return state.setIn(['topologyParams'], action.params)
    default:
      return state
  }
}
