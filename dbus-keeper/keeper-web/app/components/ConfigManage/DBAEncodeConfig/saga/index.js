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
  DBA_ENCODE_SEARCH_API
} from '@/app/containers/ConfigManage/api'

// 导入 action types
import {
  DBA_ENCODE_ALL_SEARCH
} from '../redux/action/types'

// 导入 action
import {
  searchDbaEncodeList
} from '../redux/action'


function* searchDbaEncodeListRepos (action) {
  const requestUrl = DBA_ENCODE_SEARCH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchDbaEncodeList.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchDbaEncodeList.fail(err))
  }
}

function* DbaConfigManage () {
  yield [
    yield takeLatest(DBA_ENCODE_ALL_SEARCH.LOAD, searchDbaEncodeListRepos),
  ]
}

// All sagas to be loaded
export default [DbaConfigManage]
