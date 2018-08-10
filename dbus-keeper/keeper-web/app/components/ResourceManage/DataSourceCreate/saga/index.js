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
  LATEST_JAR_GET_PATH_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  LATEST_JAR_GET_PATH
} from '../redux/action/types'

// 导入 action
import {
  getLatestJarPath
} from '../redux/action'

// 最新jar包路径
function* getLatestJarPathRepos (action) {
  const requestUrl = LATEST_JAR_GET_PATH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      getLatestJarPath.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getLatestJarPath.fail(err))
  }
}

function* DataSourceCreate () {
  yield [
    yield takeLatest(LATEST_JAR_GET_PATH.LOAD, getLatestJarPathRepos),
  ]
}

// All sagas to be loaded
export default [DataSourceCreate]
