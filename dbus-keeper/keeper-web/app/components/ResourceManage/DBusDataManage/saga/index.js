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
  DBUS_DATA_SEARCH_FROM_SOURCE_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  SEARCH_FROM_SOURCE
} from '../redux/action/types'

// 导入 action
import {
  searchFromSource
} from '../redux/action'


//查询DataSchema信息
function* searchFromSourceRepos (action) {
  const requestUrl = DBUS_DATA_SEARCH_FROM_SOURCE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchFromSource.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchFromSource.fail(err))
  }
}

function* DBusDataManage () {
  yield [
    yield takeLatest(SEARCH_FROM_SOURCE.LOAD, searchFromSourceRepos),
  ]
}

// All sagas to be loaded
export default [DBusDataManage]
