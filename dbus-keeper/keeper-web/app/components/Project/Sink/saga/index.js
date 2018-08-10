/**
 * @author 戎晓伟
 * @description saga
 */

import { message } from 'antd'
import { call, put } from 'redux-saga/effects'
import { takeLatest } from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import { RESOURCE_MANAGE_API, RESOURCE_MANAGE_PROJECT_API, RESOURCE_MANAGE_DSNAME_API } from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_RESOURCE_ALL_SEARCH,
  PROJECT_RESOURCE_PROJECT_LIST_SEARCH,
  PROJECT_RESOURCE_DSNAME_LIST_SEARCH
} from '../redux/action/types'


// 导入 action
import { searchAllResource, searchAllResourceProject, searchAllResourceDsName } from '../redux/action'

// Resource管理查询
function* getResourceListRepos (action) {
  const requestUrl = RESOURCE_MANAGE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllResource.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllResource.fail(err))
    message.error(err, 2)
  }
}


// Resource project查询
function* getResourceProjectListRepos (action) {
  const requestUrl = RESOURCE_MANAGE_PROJECT_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllResourceProject.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllResourceProject.fail(err))
    message.error(err, 2)
  }
}

// Resource dsName查询
function* getResourceDsNameListRepos (action) {
  const requestUrl = RESOURCE_MANAGE_DSNAME_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchAllResourceDsName.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchAllResourceDsName.fail(err))
    message.error(err, 2)
  }
}
function* ProjectResource () {
  yield [
    yield takeLatest(PROJECT_RESOURCE_ALL_SEARCH.LOAD, getResourceListRepos),
    yield takeLatest(PROJECT_RESOURCE_PROJECT_LIST_SEARCH.LOAD, getResourceProjectListRepos),
    yield takeLatest(PROJECT_RESOURCE_DSNAME_LIST_SEARCH.LOAD, getResourceDsNameListRepos)
  ]
}

// All sagas to be loaded
export default [ProjectResource]
