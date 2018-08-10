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
  UPDATE_GLOBAL_CONF_API
} from '@/app/containers/ConfigManage/api'

// 导入 action types
import {
  UPDATE_GLOBAL_CONF
} from '../redux/action/types'

// 导入 action
import {
  updateGlobalConf
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

function* GlobalConfigManage () {
  yield [
    yield takeLatest(UPDATE_GLOBAL_CONF.LOAD, updateGlobalConfRepos),
  ]
}

// All sagas to be loaded
export default [GlobalConfigManage]
