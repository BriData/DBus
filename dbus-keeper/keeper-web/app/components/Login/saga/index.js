/**
 * @author 戎晓伟
 * @description saga
 */

import {message} from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {
  LIST_API
} from '@/app/containers/Login/api'

// 导入 action types
import { LOGIN_GET_LIST } from '../redux/action/types'

// 导入 action
import { loginList } from '../redux/action'

// 模拟请求 saga
function* getLoginListRepos (action) {
  const requestUrl = LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(loginList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== '200') {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(loginList.fail(err))
    message.error(err, 2)
  }
}

function* login () {
  yield [
    yield takeLatest(LOGIN_GET_LIST.LOAD, getLoginListRepos)
  ]
}

// All sagas to be loaded
export default [login]
