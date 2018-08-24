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
  DATA_TABLE_SEARCH_API,
  DATA_TABLE_FIND_ALL_SEARCH_API,
  DATA_TABLE_GET_ENCODE_CONFIG_API,
  DATA_TABLE_GET_TABLE_COLUMN_API,
  DATA_TABLE_GET_ENCODE_TYPE_API,
  DATA_TABLE_GET_VERSION_LIST_API,
  DATA_TABLE_GET_VERSION_DETAIL_API,
  DATA_TABLE_SOURCE_INSIGHT_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  DATA_TABLE_ALL_SEARCH,
  DATA_TABLE_FIND_ALL_SEARCH,
  DATA_TABLE_GET_ENCODE_CONFIG,
  DATA_TABLE_GET_TABLE_COLUMN,
  DATA_TABLE_GET_ENCODE_TYPE,
  DATA_TABLE_GET_VERSION_LIST,
  DATA_TABLE_GET_VERSION_DETAIL,
  DATA_TABLE_SOURCE_INSIGHT
} from '../redux/action/types'

// 导入 action
import {
  findAllDataTableList,
  searchDataTableList,
  getEncodeConfig,
  getTableColumn,
  getEncodeType,
  getVersionList,
  getVersionDetail,
  getSourceInsight
} from '../redux/action'

//查询DataTable信息
function* searchDataTableRepos (action) {
  const requestUrl = DATA_TABLE_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchDataTableList.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDataTableList.fail(err))
  }
}

//查询DataTable信息
function* findAllDataTableListRepos (action) {
  const requestUrl = DATA_TABLE_FIND_ALL_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      findAllDataTableList.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(findAllDataTableList.fail(err))
  }
}

//查询DataTable 脱敏配置
function* getEncodeConfigRepos (action) {
  const requestUrl = DATA_TABLE_GET_ENCODE_CONFIG_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getEncodeConfig.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getEncodeConfig.fail(err))
  }
}

//查询DataTable 列meta
function* getTableColumnRepos (action) {
  const requestUrl = DATA_TABLE_GET_TABLE_COLUMN_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getTableColumn.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTableColumn.fail(err))
  }
}

//查询DataTable 脱敏类型
function* getEncodeTypeRepos (action) {
  const requestUrl = DATA_TABLE_GET_ENCODE_TYPE_API
  try {
    const repos = yield call(Request, `${requestUrl}`, {
      method: 'get'
    })
    yield put(
      getEncodeType.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getEncodeType.fail(err))
  }
}

// 查询版本列表
function* getVersionListRepos (action) {
  const requestUrl = DATA_TABLE_GET_VERSION_LIST_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getVersionList.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      const receivedVersionList = repos.payload
      if (receivedVersionList.length >= 2) {
        yield put(getVersionDetail.request({
          versionId1: receivedVersionList[1].id,
          versionId2: receivedVersionList[0].id
        }))
      } else if (receivedVersionList.length === 1) {
        yield put(getVersionDetail.request({
          versionId1: receivedVersionList[0].id,
          versionId2: receivedVersionList[0].id
        }))
        message.info('只有一个版本信息')
      } else {
        message.info('没有版本信息')
      }
    }
  } catch (err) {
    yield put(getVersionList.fail(err))
  }
}

// 查询版本详细信息
function* getVersionDetailRepos (action) {
  const requestUrl = DATA_TABLE_GET_VERSION_DETAIL_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.versionId1}/${action.result.versionId2}`, {
      method: 'get'
    })
    yield put(
      getVersionDetail.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getVersionDetail.fail(err))
  }
}
// 源端查看列信息
function* getSourceInsightRepos (action) {
  const requestUrl = DATA_TABLE_SOURCE_INSIGHT_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.tableId}/${action.result.number}`, {
      method: 'get'
    })
    yield put(
      getSourceInsight.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getSourceInsight.fail(err))
  }
}

function* DataTableManage () {
  yield [
    yield takeLatest(DATA_TABLE_ALL_SEARCH.LOAD, searchDataTableRepos),
    yield takeLatest(DATA_TABLE_GET_ENCODE_CONFIG.LOAD, getEncodeConfigRepos),
    yield takeLatest(DATA_TABLE_GET_TABLE_COLUMN.LOAD, getTableColumnRepos),
    yield takeLatest(DATA_TABLE_GET_ENCODE_TYPE.LOAD, getEncodeTypeRepos),
    yield takeLatest(DATA_TABLE_GET_VERSION_LIST.LOAD, getVersionListRepos),
    yield takeLatest(DATA_TABLE_GET_VERSION_DETAIL.LOAD, getVersionDetailRepos),
    yield takeLatest(DATA_TABLE_SOURCE_INSIGHT.LOAD, getSourceInsightRepos),
    yield takeLatest(DATA_TABLE_FIND_ALL_SEARCH.LOAD, findAllDataTableListRepos),
  ]
}

// All sagas to be loaded
export default [DataTableManage]
