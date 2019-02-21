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
  SEARCH_TABLE_LIST_API,
  SEARCH_TABLE_TOPOLOGY_LIST_API,
  SEARCH_TABLE_PROJECT_LIST_API,
  SEARCH_TABLE_DATASOURCE_LIST_API,
  SEARCH_TABLE_RESOURCES_LIST_API,
  SEARCH_TABLE_RESOURCES_COLUMNS_LIST_API,
  GET_TABLE_TOPIC_API,
  GET_TABLE_SINK_API,
  GET_TABLE_PARTITION_API,
  GET_TABLE_AFFECT_TABLE_API,
  GET_TABLE_INFO_API,
  RELOAD_TABLE_API,
  GET_TABLE_PROJECT_TOPO_API
} from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_TABLE_ALL_SEARCH,
  PROJECT_TABLE_PROJECT_LIST,
  PROJECT_TABLE_TOPOLOGY_LIST,
  PROJECT_TABLE_DATASOURCE_LIST,
  PROJECT_GET_TABLEINFO,
  PROJECT_TABLE_RESOURCELIST,
  PROJECT_TABLE_COLUMNS,
  PROJECT_TABLE_SINKS,
  PROJECT_TABLE_TOPICS,
  PROJECT_TABLE_PARTITIONS,
  PROJECT_TABLE_AFFECT_TABLES,
  PROJECT_TABLE_RELOAD,
  PROJECT_TABLE_ALL_TOPO
} from '../redux/action/types'

// 导入 action
import {
  searchTableList,
  getProjectList,
  getTopologyList,
  getDataSourceList,
  getTableInfo,
  getResourceList,
  getColumns,
  getTableSinks,
  getTableTopics,
  getTablePartitions,
  getTableAffectTables,
  sendTableReloadMsg,
  getTableProjectAllTopo
} from '../redux/action'

// Table管理查询
function* getTableListRepos (action) {
  const requestUrl = SEARCH_TABLE_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchTableList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchTableList.fail(err))
  }
}
// 获取project
function* getProjectListRepos (action) {
  const requestUrl = SEARCH_TABLE_PROJECT_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getProjectList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getProjectList.fail(err))
  }
}
// 获取dataSource
function* getDataSourceListRepos (action) {
  const requestUrl = SEARCH_TABLE_DATASOURCE_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getDataSourceList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getDataSourceList.fail(err))
  }
}
// 获取Topology
function* getTopologyListRepos (action) {
  const requestUrl = SEARCH_TABLE_TOPOLOGY_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTopologyList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopologyList.fail(err))
  }
}
// 获取TableInfo
function* getTableInfoRepos (action) {
  const requestUrl = GET_TABLE_INFO_API
  try {
    const repos = yield call(
      Request,
      `${requestUrl}/${action.result && action.result.projectId}/${action.result && action.result.id}`
    )
    yield put(
      getTableInfo.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableInfo.fail(err))
  }
}

// 新建  resourceList
function* getReSourceListRepos (action) {
  const requestUrl = SEARCH_TABLE_RESOURCES_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getResourceList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getResourceList.fail(err))
  }
}

// 新建  resourceList COLUMNS
function* getReSourceColumnsRepos (action) {
  const requestUrl = SEARCH_TABLE_RESOURCES_COLUMNS_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getColumns.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      if(repos.payload && repos.payload.length === 0) {
        message.warn('该表无列信息', 2)
      }
    }
  } catch (err) {
    yield put(getColumns.fail(err))
  }
}

// 新建获取Sink
function* getTableSinksRepos (action) {
  const requestUrl = GET_TABLE_SINK_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: { projectId: action.result && action.result.projectId },
      method: 'get'
    })
    yield put(
      getTableSinks.success((repos.status === 0 && repos.payload) || null)
    )
    const sinkId =
      (action.result && action.result.sinkId) ||
      (repos.status === 0 && repos.payload && repos.payload[0].id)
    yield put(getTableTopics.request({ projectId: action.result.projectId }))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableSinks.fail(err))
  }
}

// 新建获取Topic
function* getTableTopicRepos (action) {
  const requestUrl = GET_TABLE_TOPIC_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTableTopics.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableTopics.fail(err))
  }
}

// 根据topic,获取partition信息
function* getTablePartitionRepos (action) {
  const requestUrl = GET_TABLE_PARTITION_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTablePartitions.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTablePartitions.fail(err))
  }
}

// 根据topic,tableId 获取受影响的表
function* getTableAffectTableRepos (action) {
  const requestUrl = GET_TABLE_AFFECT_TABLE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTableAffectTables.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableAffectTables.fail(err))
  }
}

// 根据tableId 发送Reload消息
function* sendTableReloadMsgRepos (action) {
  const requestUrl = RELOAD_TABLE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      sendTableReloadMsg.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(sendTableReloadMsg.fail(err))
  }
}
// 新建获取projectID下所有的topo
function* getTableProjectTopoRepos (action) {
  const requestUrl = GET_TABLE_PROJECT_TOPO_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getTableProjectAllTopo.success(
        (repos.status === 0 && repos.payload) || null
      )
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableProjectAllTopo.fail(err))
  }
}

function* ProjectTable () {
  yield [
    yield takeLatest(PROJECT_TABLE_ALL_SEARCH.LOAD, getTableListRepos),
    yield takeLatest(PROJECT_TABLE_PROJECT_LIST.LOAD, getProjectListRepos),
    yield takeLatest(PROJECT_TABLE_TOPOLOGY_LIST.LOAD, getTopologyListRepos),
    yield takeLatest(
      PROJECT_TABLE_DATASOURCE_LIST.LOAD,
      getDataSourceListRepos
    ),
    yield takeLatest(PROJECT_GET_TABLEINFO.LOAD, getTableInfoRepos),
    yield takeLatest(PROJECT_TABLE_RESOURCELIST.LOAD, getReSourceListRepos),
    yield takeLatest(PROJECT_TABLE_COLUMNS.LOAD, getReSourceColumnsRepos),
    yield takeLatest(PROJECT_TABLE_SINKS.LOAD, getTableSinksRepos),
    yield takeLatest(PROJECT_TABLE_TOPICS.LOAD, getTableTopicRepos),
    yield takeLatest(PROJECT_TABLE_PARTITIONS.LOAD, getTablePartitionRepos),
    yield takeLatest(
      PROJECT_TABLE_AFFECT_TABLES.LOAD,
      getTableAffectTableRepos
    ),
    yield takeLatest(PROJECT_TABLE_RELOAD.LOAD, sendTableReloadMsgRepos),
    yield takeLatest(PROJECT_TABLE_ALL_TOPO.LOAD, getTableProjectTopoRepos)
  ]
}

// All sagas to be loaded
export default [ProjectTable]
