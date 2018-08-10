/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import { FULLPULL_HISTORY_LIST_API, FULLPULL_HISTORY_LIST_PROJECT_API, FULLPULL_HISTORY_LIST_DSNAME_API} from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_FULLPULL_HISTORY_ALL_SEARCH,
  PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH,
  PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH
} from '../redux/action/types'

// 导入 action
import { searchAllFullpull, searchAllFullpullProject, searchAllFullpullDsName } from '../redux/action'

// Fullpull管理查询
function* getFullpullListRepos (action) {
  const requestUrl = FULLPULL_HISTORY_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllFullpull.success((repos.status === 0 && repos.payload) || null))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllFullpull.fail(err))
    message.error(err, 2)
  }
}
// Fullpull project查询
function* getFullpullProjectListRepos (action) {
  const requestUrl = FULLPULL_HISTORY_LIST_PROJECT_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllFullpullProject.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllFullpullProject.fail(err))
    message.error(err, 2)
  }
}

// Fullpull dsName查询
function* getFullpullDsNameListRepos (action) {
  const requestUrl = FULLPULL_HISTORY_LIST_DSNAME_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllFullpullDsName.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllFullpullDsName.fail(err))
    message.error(err, 2)
  }
}
function* ProjectFullpull () {
  yield [
    yield takeLatest(PROJECT_FULLPULL_HISTORY_ALL_SEARCH.LOAD, getFullpullListRepos),
    yield takeLatest(PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.LOAD, getFullpullProjectListRepos),
    yield takeLatest(PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.LOAD, getFullpullDsNameListRepos)
  ]
}

// All sagas to be loaded
export default [ProjectFullpull]
