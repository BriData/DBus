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
  LOAD_LEVEL_OF_PATH_API,
  READ_ZK_DATA_API,
  SAVE_ZK_DATA_API,
  READ_ZK_PROPERTIES_API,
  SAVE_ZK_PROPERTIES_API,
  LOAD_ZK_TREE_BY_DSNAME_API
} from '@/app/containers/ConfigManage/api'

// 导入 action types
import {
  ZK_MANAGE_LOAD_LEVEL_OF_PATH,
  READ_ZK_DATA,
  SAVE_ZK_DATA,
  READ_ZK_PROPERTIES,
  SAVE_ZK_PROPERTIES,
  LOAD_ZK_TREE_BY_DSNAME
} from '../redux/action/types'

// 导入 action
import {
  loadLevelOfPath,
  readZkData,
  saveZkData,
  readZkProperties,
  saveZkProperties,
  loadZkTreeByDsName
} from '../redux/action'


function* loadLevelOfPathRepos (action) {
  const requestUrl = LOAD_LEVEL_OF_PATH_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      loadLevelOfPath.success(
        (repos.status === 0 && repos.payload) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(loadLevelOfPath.fail(err))
  }
}

function* readZkDataRepos (action) {
  const requestUrl = READ_ZK_DATA_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      readZkData.success(
        (repos.status === 0 && repos) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(readZkData.fail(err))
  }
}

function* saveZkDataRepos (action) {
  const requestUrl = SAVE_ZK_DATA_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: {
        path: action.result.path
      },
      data: {
        content: action.result.content
      },
      method: 'post'
    })
    yield put(
      saveZkData.success(
        (repos.status === 0 && repos) || null)
    )
    yield put(readZkData.request({ path: action.result.path }))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(saveZkData.fail(err))
  }
}

function* readZkPropertiesRepos (action) {
  const requestUrl = READ_ZK_PROPERTIES_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      readZkProperties.success(
        (repos.status === 0 && repos) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(readZkProperties.fail(err))
  }
}

function* saveZkPropertiesRepos (action) {
  const requestUrl = SAVE_ZK_PROPERTIES_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: {
        path: action.result.path
      },
      data: action.result.content,
      method: 'post'
    })
    yield put(
      saveZkProperties.success(
        (repos.status === 0 && repos) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    } else {
      message.success(repos.message)
    }
  } catch (err) {
    yield put(saveZkProperties.fail(err))
  }
}


function* loadZkTreeByDsNameRepos (action) {
  const requestUrl = LOAD_ZK_TREE_BY_DSNAME_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(
      loadZkTreeByDsName.success(
        (repos.status === 0 && repos) || null)
    )
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(loadZkTreeByDsName.fail(err))
  }
}

function* ZKManage () {
  yield [
    yield takeLatest(ZK_MANAGE_LOAD_LEVEL_OF_PATH.LOAD, loadLevelOfPathRepos),
    yield takeLatest(READ_ZK_DATA.LOAD, readZkDataRepos),
    yield takeLatest(SAVE_ZK_DATA.LOAD, saveZkDataRepos),
    yield takeLatest(READ_ZK_PROPERTIES.LOAD, readZkPropertiesRepos),
    yield takeLatest(SAVE_ZK_PROPERTIES.LOAD, saveZkPropertiesRepos),
    yield takeLatest(LOAD_ZK_TREE_BY_DSNAME.LOAD, loadZkTreeByDsNameRepos),
  ]
}

// All sagas to be loaded
export default [ZKManage]
