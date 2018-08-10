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
  SEARCH_PROJECT_API,
  DELETE_PROJECT_API,
  ENABLE_DISABLE_PROJECT_API
} from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_GET_LIST,
  DELETE_PROJECT,
  ENABLE_DISABLE_PROJECT
} from '../redux/action/types'

// 导入 action
import { searchProject, deleteProject, enableDisableProject } from '../redux/action'

// 查询项目列表
function* getProjectListRepos (action) {
  const requestUrl = SEARCH_PROJECT_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchProject.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchProject.fail(err))
  }
}

// 查询项目信息
function* deleteProjectRepos (action) {
  const requestUrl = DELETE_PROJECT_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      deleteProject.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(deleteProject.fail(err))
  }
}

// 启用禁用项目
function* enableDisableProjectRepos (action) {
  const requestUrl = ENABLE_DISABLE_PROJECT_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'post'
    })
    yield put(
      enableDisableProject.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(enableDisableProject.fail(err))
  }
}

function* ProjectSummary () {
  yield [
    yield takeLatest(PROJECT_GET_LIST.LOAD, getProjectListRepos),
    yield takeLatest(DELETE_PROJECT.LOAD, deleteProjectRepos),
    yield takeLatest(ENABLE_DISABLE_PROJECT.LOAD, enableDisableProjectRepos)
  ]
}

// All sagas to be loaded
export default [ProjectSummary]
