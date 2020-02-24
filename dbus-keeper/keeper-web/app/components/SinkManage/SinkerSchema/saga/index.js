/**
 * @author 戎晓伟
 * @description saga
 */

import {message} from 'antd'
import {call, put} from 'redux-saga/effects'
import {takeLatest} from 'redux-saga'
import Request from '@/app/utils/request'
// 导入API
import {SEARCH_SINKER_SCHEMA_API} from '@/app/containers/SinkManage/api'
// 导入 action types
import {SEARCH_SINKER_SCHEMA_LIST} from '../redux/action/types'
// 导入 action
import {searchSinkerSchemaList} from '../redux/action'

// SinkerSchema管理查询
function* getSinkerSchemaListRepos(action) {
  const requestUrl = SEARCH_SINKER_SCHEMA_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchSinkerSchemaList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchSinkerSchemaList.fail(err))
    message.error(err, 2)
  }
}

function* SinkerSchema() {
  yield [
    yield takeLatest(SEARCH_SINKER_SCHEMA_LIST.LOAD, getSinkerSchemaListRepos)
  ]
}

// All sagas to be loaded
export default [SinkerSchema]
