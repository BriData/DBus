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
  ENCODE_MANAGER_SEARCH_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  ENCODE_MANAGER_SEARCH,
} from '../redux/action/types'

// 导入 action
import {
  searchEncodeList
} from '../redux/action'

// Table管理查询
function* searchEncodeListRepos (action) {
  const requestUrl = ENCODE_MANAGER_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchEncodeList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchEncodeList.fail(err))
  }
}

function* EncodeManager () {
  yield [
    yield takeLatest(ENCODE_MANAGER_SEARCH.LOAD, searchEncodeListRepos),
  ]
}

// All sagas to be loaded
export default [EncodeManager]
