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
  SEARCH_JAR_INFOS_API
} from '@/app/containers/ResourceManage/api'

// 导入 action types
import {
  SEARCH_JAR_INFOS
} from '../redux/action/types'

// 导入 action
import { searchJarInfos } from '../redux/action'

// 查询jar包信息
function* searchJarInfosRepos (action) {
  const requestUrl = SEARCH_JAR_INFOS_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchJarInfos.success(repos)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchJarInfos.fail(err))
  }
}

function* JarManage () {
  yield [
    yield takeLatest(SEARCH_JAR_INFOS.LOAD, searchJarInfosRepos),
  ]
}

// All sagas to be loaded
export default [JarManage]
