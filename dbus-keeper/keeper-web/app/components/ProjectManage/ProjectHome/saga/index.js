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
  SEARCH_USER_API,
  SEARCH_SINK_API,
  SEARCH_RESOURCE_API,
  SEARCH_ENCODE_API,
  SEARCH_ENCODE_SELECT_API,
  GET_PROJECT_INFO_API
} from '@/app/containers/ProjectManage/api'

// 导入 action types
import {
  PROJECT_USER_SEARCH,
  PROJECT_SINK_SEARCH,
  PROJECT_RESOURCE_SEARCH,
  RESOURCE_ENCODE_LIST,
  RESOURCE_ENCODE_TYPE_LIST,
  GET_PROJECT_INFO
} from '../redux/action/types'

// 导入 action
import { searchUser, searchSink, searchResource, searchEncode, getEncodeTypeList, getProjectInfo } from '../redux/action'

// 查询用户
function* getUserListRepos (action) {
  const requestUrl = SEARCH_USER_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchUser.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchUser.fail(err))
  }
}
// 查询Sink
function* getSinkListRepos (action) {
  const requestUrl = SEARCH_SINK_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      searchSink.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchSink.fail(err))
  }
}
// 查询 Resource
function* getResourceListRepos (action) {
  const requestUrl = SEARCH_RESOURCE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchResource.success((repos.status === 0 && repos.payload) || null))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchResource.fail(err))
  }
}
// 查询 脱敏列表
function* getEncodeRepos (action) {
  const requestUrl = SEARCH_ENCODE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(searchEncode.success((repos.status === 0 && repos.payload) || null))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(searchEncode.fail(err))
  }
}
// 获取 脱敏规则-下拉列表
function* getEncodeTypeListRepos (action) {
  const requestUrl = SEARCH_ENCODE_SELECT_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(getEncodeTypeList.success((repos.status === 0 && repos.payload) || null))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getEncodeTypeList.fail(err))
  }
}

// 查询项目信息
function* getProjectInfoRepos (action) {
  const requestUrl = GET_PROJECT_INFO_API
  try {
    const repos = yield call(Request, `${requestUrl}/${action.result.id}`, {
      method: 'get'
    })
    yield put(
      getProjectInfo.success((repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getProjectInfo.fail(err))
  }
}

function* ProjectHome () {
  yield [
    yield takeLatest(PROJECT_USER_SEARCH.LOAD, getUserListRepos),
    yield takeLatest(PROJECT_SINK_SEARCH.LOAD, getSinkListRepos),
    yield takeLatest(PROJECT_RESOURCE_SEARCH.LOAD, getResourceListRepos),
    yield takeLatest(RESOURCE_ENCODE_LIST.LOAD, getEncodeRepos),
    yield takeLatest(RESOURCE_ENCODE_TYPE_LIST.LOAD, getEncodeTypeListRepos),
    yield takeLatest(GET_PROJECT_INFO.LOAD, getProjectInfoRepos)
  ]
}

// All sagas to be loaded
export default [ProjectHome]
