/**
 * @author 戎晓伟
 * @description saga
 */

import {message} from 'antd'
import {call, put} from 'redux-saga/effects'
import {takeLatest} from 'redux-saga'
import Request from '@/app/utils/request'
// 导入API
import {
  DATA_SOURCE_CLEAR_FULLPULL_ALARM_API,
  DATA_SOURCE_DELETE_API,
  DATA_SOURCE_GET_BY_ID_API,
  DATA_SOURCE_GET_CANAL_CONF_API,
  DATA_SOURCE_GET_ID_TYPE_NAME_API,
  DATA_SOURCE_GET_OGG_CONF_API,
  DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID_API,
  DATA_SOURCE_GET_SCHEMA_TABLE_LIST_API,
  DATA_SOURCE_INSERT_API,
  DATA_SOURCE_SEARCH_API,
  DATA_SOURCE_UPDATE_API,
  KILL_TOPOLOGY_API
} from '@/app/containers/ResourceManage/api'
// 导入 action types
import {
  DATA_SOURCE_ALL_SEARCH,
  DATA_SOURCE_CLEAR_FULLPULL_ALARM,
  DATA_SOURCE_DELETE,
  DATA_SOURCE_GET_BY_ID,
  DATA_SOURCE_GET_ID_TYPE_NAME,
  DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID,
  DATA_SOURCE_GET_SCHEMA_TABLE_LIST,
  DATA_SOURCE_INSERT,
  DATA_SOURCE_UPDATE,
  KILL_TOPOLOGY,
  OGG_CANAL_CONF_GET_BY_DS_NAME
} from '../redux/action/types'
// 导入 action
import {
  clearFullPullAlarm,
  deleteDataSource,
  getDataSourceById,
  getOggCanalConfByDsName,
  getSchemaListByDsId,
  getSchemaTableList,
  insertDataSource,
  killTopology,
  searchDataSourceIdTypeName,
  searchDataSourceList,
  searchJarInfos,
  updateDataSource
} from '../redux/action'

//查询DataSource信息
function* clearFullPullAlarmRepos(action) {
  const requestUrl = DATA_SOURCE_CLEAR_FULLPULL_ALARM_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      clearFullPullAlarm.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(clearFullPullAlarm.fail(err))
  }
}

//查询DataSource信息
function* searchDataSourceRepos(action) {
  const requestUrl = DATA_SOURCE_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchDataSourceList.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDataSourceList.fail(err))
  }
}

// DataSource名称和id的列表，提供给schema级应用使用
function* searchDataSourceIdTypeNameRepos(action) {
  const requestUrl = DATA_SOURCE_GET_ID_TYPE_NAME_API
  try {
    const repos = yield call(Request, requestUrl, {
      method: 'get'
    })
    yield put(
      searchDataSourceIdTypeName.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDataSourceIdTypeName.fail(err))
  }
}

//查询DataSource by id
function* getDataSourceByIdRepos(action) {
  const requestUrl = DATA_SOURCE_GET_BY_ID_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getDataSourceById.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getDataSourceById.fail(err))
  }
}

//删除DataSource信息
function* deleteDataSourceRepos(action) {
  const requestUrl = DATA_SOURCE_DELETE_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      deleteDataSource.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(deleteDataSource.fail(err))
  }
}


//修改DataSource信息
function* updateDataSourceRepos(action) {
  const requestUrl = DATA_SOURCE_UPDATE_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(
      updateDataSource.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(updateDataSource.fail(err))
  }
}


//增加DataSource信息
function* insertDataSourceRepos(action) {
  const requestUrl = DATA_SOURCE_INSERT_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(
      insertDataSource.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(insertDataSource.fail(err))
  }
}

//topology kill
function* killTopologyRepos(action) {
  const requestUrl = KILL_TOPOLOGY_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.topologyId}/${action.result.waitTime}`, {
      method: 'post'
    })
    yield put(
      killTopology.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(killTopology.fail(err))
  }
}

// 获取schema list by dsId
function* getSchemaListByDsIdRepos(action) {
  const requestUrl = DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getSchemaListByDsId.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getSchemaListByDsId.fail(err))
  }
}

// 获取schema table 信息
function* getSchemaTableListRepos(action) {
  const requestUrl = DATA_SOURCE_GET_SCHEMA_TABLE_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getSchemaTableList.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getSchemaTableList.fail(err))
  }
}

// 获取schema table 信息
function* getSchemaTableListRepos(action) {
  const requestUrl = DATA_SOURCE_GET_SCHEMA_TABLE_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getSchemaTableList.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getSchemaTableList.fail(err))
  }
}

// 获取oggConf 信息
function* getOggCanalConfByDsNameRepos(action) {
  let requestUrl = '';
  if (action.result.type === 'mysql') {
    requestUrl = DATA_SOURCE_GET_CANAL_CONF_API
  }
  if (action.result.type === 'oracle') {
    requestUrl = DATA_SOURCE_GET_OGG_CONF_API
  }
  try {
    const repos = yield call(Request, requestUrl, {
      params: {dsName: action.result.name},
      method: 'get'
    })
    yield put(
      getOggCanalConfByDsName.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getOggCanalConfByDsName.fail(err))
  }
}

function* DataSourceNamage() {
  yield [
    yield takeLatest(DATA_SOURCE_ALL_SEARCH.LOAD, searchDataSourceRepos),
    yield takeLatest(DATA_SOURCE_GET_ID_TYPE_NAME.LOAD, searchDataSourceIdTypeNameRepos),
    yield takeLatest(DATA_SOURCE_DELETE.LOAD, deleteDataSourceRepos),
    yield takeLatest(DATA_SOURCE_UPDATE.LOAD, updateDataSourceRepos),
    yield takeLatest(DATA_SOURCE_INSERT.LOAD, insertDataSourceRepos),
    yield takeLatest(DATA_SOURCE_GET_BY_ID.LOAD, getDataSourceByIdRepos),
    yield takeLatest(KILL_TOPOLOGY.LOAD, killTopologyRepos),
    yield takeLatest(DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.LOAD, getSchemaListByDsIdRepos),
    yield takeLatest(DATA_SOURCE_GET_SCHEMA_TABLE_LIST.LOAD, getSchemaTableListRepos),
    yield takeLatest(DATA_SOURCE_CLEAR_FULLPULL_ALARM.LOAD, clearFullPullAlarmRepos),
    yield takeLatest(OGG_CANAL_CONF_GET_BY_DS_NAME.LOAD, getOggCanalConfByDsNameRepos)
  ]
}

// All sagas to be loaded
export default [DataSourceNamage]
