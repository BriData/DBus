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
  GLOBAL_FULL_PULL_API
} from '@/app/containers/toolSet/api'

// 导入 action types
import {
  GLOBAL_FULL_PULL
} from '../redux/action/types'

// 导入 action
import {
  globalFullPull
} from '../redux/action'

function* globalFullPullRepos(action) {
  const requestUrl = GLOBAL_FULL_PULL_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(globalFullPull.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(globalFullPull.fail(err))
    message.error(err, 2)
  }
}

function* ToolSet() {
  yield [
    yield takeLatest(GLOBAL_FULL_PULL.LOAD, globalFullPullRepos),
  ]
}

export default [ToolSet]
