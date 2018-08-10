/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {SEARCH_SINK_LIST_API, CREATE_SINK_API, UPDATE_SINK_API, DELETE_SINK_API} from '@/app/containers/SinkManage/api'
// 导入 action types
import {
  SEARCH_SINK_LIST,
  CREATE_SINK,
  UPDATE_SINK,
  DELETE_SINK
} from '../redux/action/types'

// 导入 action
import { searchSinkList, createSink, updateSink, deleteSink } from '../redux/action'

// Sink管理查询
function* getSinkListRepos (action) {
  const requestUrl = SEARCH_SINK_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchSinkList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchSinkList.fail(err))
    message.error(err, 2)
  }
}

function* createSinkRepos (action) {
  const requestUrl = CREATE_SINK_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(createSink.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== '200') {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(createSink.fail(err))
    message.error(err, 2)
  }
}

function* updateSinkRepos (action) {
  const requestUrl = UPDATE_SINK_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(updateSink.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== '200') {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(updateSink.fail(err))
    message.error(err, 2)
  }
}

function* deleteSinkRepos (action) {
  const requestUrl = DELETE_SINK_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(deleteSink.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== '200') {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(deleteSink.fail(err))
    message.error(err, 2)
  }
}

function* SinkHome () {
  yield [
    yield takeLatest(SEARCH_SINK_LIST.LOAD, getSinkListRepos),
    yield takeLatest(CREATE_SINK.LOAD, createSinkRepos),
    yield takeLatest(UPDATE_SINK.LOAD, updateSinkRepos),
    yield takeLatest(DELETE_SINK.LOAD, deleteSinkRepos)
  ]
}

// All sagas to be loaded
export default [SinkHome]
