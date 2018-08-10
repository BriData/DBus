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
  DATA_SCHEMA_SEARCH_API,
  DATA_SCHEMA_GET_BY_ID_API,
  DATA_SCHEMA_SEARCH_ALL_LIST_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  DATA_SCHEMA_ALL_SEARCH,
  DATA_SCHEMA_GET_BY_ID,
  DATA_SCHEMA_SEARCH_ALL_LIST
} from '../redux/action/types'

// 导入 action
import {
  searchDataSchemaList,
  getDataSchemaById,
  searchAllDataSchema
} from '../redux/action'


//查询DataSchema信息
function* searchDataSchemaRepos (action) {
  const requestUrl = DATA_SCHEMA_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchDataSchemaList.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDataSchemaList.fail(err))
  }
}

//查询DataSchema所有信息 不分页
function* searchAllDataSchemaRepos (action) {
  const requestUrl = DATA_SCHEMA_SEARCH_ALL_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchAllDataSchema.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllDataSchema.fail(err))
  }
}

//查询DataSchema by id
function* getDataSchemaByIdRepos (action) {
  const requestUrl = DATA_SCHEMA_GET_BY_ID_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getDataSchemaById.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getDataSchemaById.fail(err))
  }
}

function* DataSchemaManage () {
  yield [
    yield takeLatest(DATA_SCHEMA_ALL_SEARCH.LOAD, searchDataSchemaRepos),
    yield takeLatest(DATA_SCHEMA_GET_BY_ID.LOAD, getDataSchemaByIdRepos),
    yield takeLatest(DATA_SCHEMA_SEARCH_ALL_LIST.LOAD, searchAllDataSchemaRepos)
  ]
}

// All sagas to be loaded
export default [DataSchemaManage]
