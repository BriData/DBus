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
  UPDATE_GLOBAL_CONF_API,
  INIT_GLOBAL_CONF_API
} from '@/app/containers/ConfigManage/api'

// 导入 action types
import {
  UPDATE_GLOBAL_CONF,
  INIT_GLOBAL_CONF
} from '../redux/action/types'

// 导入 action
import {
  updateGlobalConf,
  initGlobalConf
} from '../redux/action'


function* updateGlobalConfRepos (action) {
  const requestUrl = UPDATE_GLOBAL_CONF_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(
      updateGlobalConf.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(updateGlobalConf.fail(err))
  }
}

function* initGlobalConfRepos (action) {
  const requestUrl = INIT_GLOBAL_CONF_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: {
        options: action.result.options
      },
      data: action.result.content,
      method: 'post'
    })
    yield put(
      initGlobalConf.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(initGlobalConf.fail(err))
  }
}

function* GlobalConfigManage () {
  yield [
    yield takeLatest(UPDATE_GLOBAL_CONF.LOAD, updateGlobalConfRepos),
    yield takeLatest(INIT_GLOBAL_CONF.LOAD, initGlobalConfRepos),
  ]
}

// All sagas to be loaded
export default [GlobalConfigManage]
