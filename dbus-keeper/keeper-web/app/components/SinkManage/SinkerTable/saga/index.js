/**
 * @author 戎晓伟
 * @description saga
 */

import {message} from 'antd'
import {call, put} from 'redux-saga/effects'
import {takeLatest} from 'redux-saga'
import Request from '@/app/utils/request'
// 导入API
import {SEARCH_SINKER_TABLE_API} from '@/app/containers/SinkManage/api'
// 导入 action types
import {SEARCH_SINKER_TABLE_LIST} from '../redux/action/types'
// 导入 action
import {searchSinkerTableList} from '../redux/action'

// SinkerTable管理查询
function* getSinkerTableListRepos(action) {
  const requestUrl = SEARCH_SINKER_TABLE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchSinkerTableList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchSinkerTableList.fail(err))
    message.error(err, 2)
  }
}

function* SinkerTable() {
  yield [
    yield takeLatest(SEARCH_SINKER_TABLE_LIST.LOAD, getSinkerTableListRepos)
  ]
}

// All sagas to be loaded
export default [SinkerTable]
