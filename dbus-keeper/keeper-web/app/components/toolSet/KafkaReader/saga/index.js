/**
 * @author xiancangao
 * @description saga
 */

import {message} from 'antd'
import {call, put} from 'redux-saga/effects'
import {takeLatest} from 'redux-saga'
import Request from '@/app/utils/request'

// 导入API
import {
  KAFKA_READER_GET_TOPIC_LIST_API,
  KAFKA_READER_READ_DATA_API,
  KAFKA_READER_GET_OFFSET_RANGE_API,
  KAFKA_READER_GET_TOPICS_BY_USER_ID_API
} from '@/app/containers/toolSet/api'

// 导入 action types
import {
  KAFKA_READER_GET_TOPIC_LIST,
  KAFKA_READER_READ_DATA,
  KAFKA_READER_GET_OFFSET_RANGE,
  KAFKA_READER_GET_TOPICS_BY_USER_ID
} from '../redux/action/types'

// 导入 action
import {
  getTopicList,
  readKafkaData,
  getOffsetRange,
  getTopicsByUserId
} from '../redux/action'

function* getTopicListRepos(action) {
  const requestUrl = KAFKA_READER_GET_TOPIC_LIST_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(getTopicList.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopicList.fail(err))
    message.error(err, 2)
  }
}

function* getTopicListsByUserIdRepos(action) {
  const requestUrl = KAFKA_READER_GET_TOPICS_BY_USER_ID_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(getTopicsByUserId.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getTopicsByUserId.fail(err))
    message.error(err, 2)
  }
}

function* readKafkaDataRepos(action) {
  const requestUrl = KAFKA_READER_READ_DATA_API
  try {
    const repos = yield call(Request, requestUrl, {
      data: action.result,
      method: 'POST'
    })
    yield put(readKafkaData.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(readKafkaData.fail(err))
    message.error(err, 2)
  }
}

function* getOffsetRangeRepos(action) {
  const requestUrl = KAFKA_READER_GET_OFFSET_RANGE_API
  try {
    const repos = yield call(Request, requestUrl, {
      params: action.result,
      method: 'get'
    })
    yield put(getOffsetRange.success(repos))
    if (repos.status === undefined) {
      message.error('网络连接错误', 2)
    } else if (repos.status !== 0) {
      message.error(repos.message || '获取失败', 2)
    }
  } catch (err) {
    yield put(getOffsetRange.fail(err))
    message.error(err, 2)
  }
}

function* KafkaReader() {
  yield [
    yield takeLatest(KAFKA_READER_GET_TOPIC_LIST.LOAD, getTopicListRepos),
    yield takeLatest(KAFKA_READER_READ_DATA.LOAD, readKafkaDataRepos),
    yield takeLatest(KAFKA_READER_GET_OFFSET_RANGE.LOAD, getOffsetRangeRepos),
    yield takeLatest(KAFKA_READER_GET_TOPICS_BY_USER_ID.LOAD, getTopicListsByUserIdRepos),
  ]
}

export default [KafkaReader]
