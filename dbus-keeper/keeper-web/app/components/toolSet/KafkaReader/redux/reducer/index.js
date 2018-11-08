/**
 * @author xiancangao
 * @description redux->reducer
 */

import { fromJS } from 'immutable'

// 导入types
import {
  KAFKA_READER_READ_DATA,
  KAFKA_READER_GET_TOPIC_LIST,
  KAFKA_READER_GET_OFFSET_RANGE,
  KAFKA_READER_GET_TOPICS_BY_USER_ID
} from '../action/types'

const initialState = fromJS({
  topicList: {
    loading: false,
    loaded: false,
    result: {}
  },
  kafkaData: {
    loading: false,
    loaded: false,
    result: {}
  },
  offsetRange: {
    loading: false,
    loaded: false,
    result: {}
  },
  topicsByUserIdList: {
    loading: false,
    loaded: false,
    result: {}
  }
})

export default (state = initialState, action) => {
  switch (action.type) {
    case KAFKA_READER_GET_TOPIC_LIST.LOAD:
      return state.setIn(['topicList', 'loading'], true)
    case KAFKA_READER_GET_TOPIC_LIST.SUCCESS:
      return state
        .setIn(['topicList', 'loading'], false)
        .setIn(['topicList', 'loaded'], true)
        .setIn(['topicList', 'result'], action.result)
    case KAFKA_READER_GET_TOPIC_LIST.FAIL:
      return state
        .setIn(['topicList', 'loading'], false)
        .setIn(['topicList', 'loaded'], true)
        .setIn(['topicList', 'result'], action.result)
    case KAFKA_READER_READ_DATA.LOAD:
      return state.setIn(['kafkaData', 'loading'], true)
    case KAFKA_READER_READ_DATA.SUCCESS:
      return state
        .setIn(['kafkaData', 'loading'], false)
        .setIn(['kafkaData', 'loaded'], true)
        .setIn(['kafkaData', 'result'], action.result)
    case KAFKA_READER_READ_DATA.FAIL:
      return state
        .setIn(['kafkaData', 'loading'], false)
        .setIn(['kafkaData', 'loaded'], true)
        .setIn(['kafkaData', 'result'], action.result)
    case KAFKA_READER_GET_OFFSET_RANGE.LOAD:
      return state.setIn(['offsetRange', 'loading'], true)
    case KAFKA_READER_GET_OFFSET_RANGE.SUCCESS:
      return state
        .setIn(['offsetRange', 'loading'], false)
        .setIn(['offsetRange', 'loaded'], true)
        .setIn(['offsetRange', 'result'], action.result)
    case KAFKA_READER_GET_OFFSET_RANGE.FAIL:
      return state
        .setIn(['offsetRange', 'loading'], false)
        .setIn(['offsetRange', 'loaded'], true)
        .setIn(['offsetRange', 'result'], action.result)
    case KAFKA_READER_GET_TOPICS_BY_USER_ID.LOAD:
      return state.setIn(['topicsByUserIdList', 'loading'], true)
    case KAFKA_READER_GET_TOPICS_BY_USER_ID.SUCCESS:
      return state
        .setIn(['topicsByUserIdList', 'loading'], false)
        .setIn(['topicsByUserIdList', 'loaded'], true)
        .setIn(['topicsByUserIdList', 'result'], action.result)
    case KAFKA_READER_GET_TOPICS_BY_USER_ID.FAIL:
      return state
        .setIn(['topicsByUserIdList', 'loading'], false)
        .setIn(['topicsByUserIdList', 'loaded'], true)
        .setIn(['topicsByUserIdList', 'result'], action.result)
    default:
      return state
  }
}
