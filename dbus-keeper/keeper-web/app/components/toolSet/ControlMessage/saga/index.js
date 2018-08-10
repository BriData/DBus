/**
 * @author xiancangao
 * @description saga
 */

import {message} from 'antd'
import {call, put} from 'redux-saga/effects'
import {takeLatest} from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {
  SEARCH_TABLE_DATASOURCE_LIST_API,
  SEND_CONTROL_MESSAGE_API,
  READ_RELOAD_INFO_API
} from '@/app/containers/toolSet/api'

// 导入 action types
import {
  DATA_SOURCE_LIST,
  SEND_CONTROL_MESSAGE,
  READ_RELOAD_INFO
} from '../redux/action/types'

// 导入 action
import {
  searchDataSourceList,
  sendControlMessage,
  readReloadInfo
} from '../redux/action'

function* getDataSourceListRepos(action) {
  const requestUrl = SEARCH_TABLE_DATASOURCE_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchDataSourceList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDataSourceList.fail(err))
    message.error(err, 2)
  }
}


function* sendControlMessageRepos(action) {
  const requestUrl = SEND_CONTROL_MESSAGE_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(sendControlMessage.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(sendControlMessage.fail(err))
    message.error(err, 2)
  }
}


function* readReloadInfoRepos(action) {
  const requestUrl = READ_RELOAD_INFO_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(readReloadInfo.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(readReloadInfo.fail(err))
    message.error(err, 2)
  }
}

function* ToolSet() {
  yield [
    yield takeLatest(DATA_SOURCE_LIST.LOAD, getDataSourceListRepos),
    yield takeLatest(SEND_CONTROL_MESSAGE.LOAD, sendControlMessageRepos),
    yield takeLatest(READ_RELOAD_INFO.LOAD, readReloadInfoRepos),
  ]
}

export default [ToolSet]
