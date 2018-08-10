/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {
  SEARCH_TOPOLOGY_LIST_API,
  GET_TOPOLOGY_INFO_API,
  SEARCH_TOPOLOGY_JAR_VERSIONS_API,
  SEARCH_TOPOLOGY_JAR_PACKAGES_API,
  PROJECT_TOPOLOGY_FEEDS_SEARCH_API,
  PROJECT_TOPOLOGY_OUTTOPIC_SEARCH_API,
  TOPOLOGY_EFFECT_API,
  TOPOLOGY_RERUN_INIT_API
} from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_TOPOLOGY_ALL_SEARCH,
  PROJECT_TOPOLOGY_JAR_VERSIONS,
  PROJECT_TOPOLOGY_JAR_PACKAGES,
  PROJECT_TOPOLOGY_INFO,
  PROJECT_TOPOLOGY_FEEDS_SEARCH,
  PROJECT_TOPOLOGY_OUTTOPIC_SEARCH,
  PROJECT_TOPOLOGY_EFFECT,
  PROJECT_TOPOLOGY_RERUN_INIT
} from '../redux/action/types'

// 导入 action
import {
  searchAllTopology,
  getTopologyJarVersions,
  getTopologyJarPackages,
  getTopologyInfo,
  getTopologyFeeds,
  getTopologyOutTopic,
  topologyEffect,
  topologyRerunInit
} from '../redux/action'

// Topology管理查询
function* getTopologyListRepos (action) {
  const requestUrl = SEARCH_TOPOLOGY_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchAllTopology.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllTopology.fail(err))
  }
}
// Topology 获取Topo信息
function* getTopologyInfoRepos (action) {
  const requestUrl = GET_TOPOLOGY_INFO_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`)
    yield put(
      getTopologyInfo.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyInfo.fail(err))
  }
}
// Topology获取JAR版本
function* getTopologyJarVersionRepos (action) {
  const requestUrl = SEARCH_TOPOLOGY_JAR_VERSIONS_API
  try {
    const repos = yield call(Request, requestUrl)
    yield put(
      getTopologyJarVersions.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    const version =
    (action.result && action.result.version) ||
    (repos.status === 0 && repos.payload && repos.payload[0])
    yield put(getTopologyJarPackages.request({ version }))

    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyJarVersions.fail(err))
  }
}
// Topology获取JAR包
function* getTopologyJarPackagesRepos (action) {
  const requestUrl = SEARCH_TOPOLOGY_JAR_PACKAGES_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTopologyJarPackages.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyJarPackages.fail(err))
  }
}

// Topology 生效
function* topologyEffectRepos (action) {
  const requestUrl = TOPOLOGY_EFFECT_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      topologyEffect.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message || 'OK', 2)
    }
  } catch (err) {
    yield put(topologyEffect.fail(err))
  }
}

// Topology rerun init
function* topologyRerunInitRepos (action) {
  const requestUrl = TOPOLOGY_RERUN_INIT_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.projectId}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      topologyRerunInit.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(topologyRerunInit.fail(err))
  }
}


// Topology 获取订阅源
function* getTopologyFeedsRepos (action) {
  const requestUrl = PROJECT_TOPOLOGY_FEEDS_SEARCH_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.projectId}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getTopologyFeeds.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyFeeds.fail(err))
  }
}

// Topology 获取输出topic列表
function* getTopologyOutTopicRepos (action) {
  const requestUrl = PROJECT_TOPOLOGY_OUTTOPIC_SEARCH_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.projectId}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getTopologyOutTopic.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyOutTopic.fail(err))
  }
}

function* ProjectTopology () {
  yield [
    yield takeLatest(PROJECT_TOPOLOGY_ALL_SEARCH.LOAD, getTopologyListRepos),
    yield takeLatest(PROJECT_TOPOLOGY_INFO.LOAD, getTopologyInfoRepos),
    yield takeLatest(
      PROJECT_TOPOLOGY_JAR_VERSIONS.LOAD,
      getTopologyJarVersionRepos
    ),
    yield takeLatest(
      PROJECT_TOPOLOGY_JAR_PACKAGES.LOAD,
      getTopologyJarPackagesRepos
    ),
    yield takeLatest(PROJECT_TOPOLOGY_FEEDS_SEARCH.LOAD, getTopologyFeedsRepos),
    yield takeLatest(PROJECT_TOPOLOGY_OUTTOPIC_SEARCH.LOAD, getTopologyOutTopicRepos),
    yield takeLatest(PROJECT_TOPOLOGY_EFFECT.LOAD, topologyEffectRepos),
    yield takeLatest(PROJECT_TOPOLOGY_RERUN_INIT.LOAD, topologyRerunInitRepos)
  ]
}

// All sagas to be loaded
export default [ProjectTopology]
