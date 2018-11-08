/**
 * @author xiancangao
 * @description redux->action
 */

// 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  KAFKA_READER_READ_DATA,
  KAFKA_READER_GET_TOPIC_LIST,
  KAFKA_READER_GET_OFFSET_RANGE, KAFKA_READER_GET_TOPICS_BY_USER_ID
} from './types'

export const getTopicList = {
  request: params => createAction(KAFKA_READER_GET_TOPIC_LIST.LOAD, { ...params }),
  success: data => createAction(KAFKA_READER_GET_TOPIC_LIST.SUCCESS, { ...data }),
  fail: error => createAction(KAFKA_READER_GET_TOPIC_LIST.FAIL, { ...error })
}

export const getTopicsByUserId = {
  request: params => createAction(KAFKA_READER_GET_TOPICS_BY_USER_ID.LOAD, { ...params }),
  success: data => createAction(KAFKA_READER_GET_TOPICS_BY_USER_ID.SUCCESS, { ...data }),
  fail: error => createAction(KAFKA_READER_GET_TOPICS_BY_USER_ID.FAIL, { ...error })
}

export const readKafkaData = {
  request: params => createAction(KAFKA_READER_READ_DATA.LOAD, { ...params }),
  success: data => createAction(KAFKA_READER_READ_DATA.SUCCESS, { ...data }),
  fail: error => createAction(KAFKA_READER_READ_DATA.FAIL, { ...error })
}

export const getOffsetRange = {
  request: params => createAction(KAFKA_READER_GET_OFFSET_RANGE.LOAD, { ...params }),
  success: data => createAction(KAFKA_READER_GET_OFFSET_RANGE.SUCCESS, { ...data }),
  fail: error => createAction(KAFKA_READER_GET_OFFSET_RANGE.FAIL, { ...error })
}

