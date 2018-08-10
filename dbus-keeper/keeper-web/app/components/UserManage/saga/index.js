/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import { SEARCH_USER_LIST_API, GET_USER_INFO_API, GET_USER_PROJECT_API } from '@/app/containers/UserManage/api'

// 导入 action types
import {
  ALL_USER_LIST_SEARCH,
  ALL_USER_INFO,
  ALL_USER_PROJECT_INFO
} from '../redux/action/types'

// 导入 action
import { searchUserList, getUserInfo, getUserProject } from '../redux/action'

// 查询用户
function* getUserManageListRepos (action) {
  const requestUrl = SEARCH_USER_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchUserList.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchUserList.fail(err))
  }
}
// 获取用户信息
function* getUserInfoRepos (action) {
  const requestUrl = GET_USER_INFO_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`)
    yield put(
      getUserInfo.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getUserInfo.fail(err))
  }
}
// 获取用户对应的项目信息
function* getUserProjectRepos (action) {
  const requestUrl = GET_USER_PROJECT_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.userId}/${action.result.roleType}`)
    yield put(
      getUserProject.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getUserProject.fail(err))
  }
}

function* UserManage () {
  yield [
    yield takeLatest(ALL_USER_LIST_SEARCH.LOAD, getUserManageListRepos),
    yield takeLatest(ALL_USER_INFO.LOAD, getUserInfoRepos),
    yield takeLatest(ALL_USER_PROJECT_INFO.LOAD, getUserProjectRepos)
  ]
}

// All sagas to be loaded
export default [UserManage]
