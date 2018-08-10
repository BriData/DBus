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
  SEARCH_ENCODE_PLUGIN_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  SEARCH_ENCODE_PLUGIN
} from '../redux/action/types'

// 导入 action
import { searchEncodePlugin } from '../redux/action'

// 查询jar包信息
function* searchEncodePluginRepos (action) {
  const requestUrl = SEARCH_ENCODE_PLUGIN_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchEncodePlugin.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchEncodePlugin.fail(err))
  }
}

function* EncodePluginManage () {
  yield [
    yield takeLatest(SEARCH_ENCODE_PLUGIN.LOAD, searchEncodePluginRepos),
  ]
}

// All sagas to be loaded
export default [EncodePluginManage]
