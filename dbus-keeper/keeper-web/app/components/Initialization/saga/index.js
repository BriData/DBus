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
  GET_BASIC_CONF_API
} from '@/app/containers/Initialization/api'

// 导入 action types
import { GET_BASIC_CONF } from '../redux/action/types'

// 导入 action
import { getBasicConf } from '../redux/action'

// 模拟请求 saga
function* getBasicConfRepos (action) {
  const requestUrl = GET_BASIC_CONF_API
  try {
    const repos = yield call(Request, requestUrl, {
      method: 'get'
    })
    yield put(getBasicConf.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getBasicConf.fail(err))
  }
}

function* initialization () {
  yield [
    yield takeLatest(GET_BASIC_CONF.LOAD, getBasicConfRepos)
  ]
}

// All sagas to be loaded
export default [initialization]
