import { createActionTypes } from "@/app/utils/createAction"

/**
 * @author xiancangao
 * @description redux->type
 */
export const KAFKA_READER_GET_TOPIC_LIST = createActionTypes('toolSet/KafkaReader/KAFKA_READER_GET_TOPIC_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const KAFKA_READER_GET_TOPICS_BY_USER_ID = createActionTypes('toolSet/KafkaReader/KAFKA_READER_GET_TOPICS_BY_USER_ID', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const KAFKA_READER_READ_DATA = createActionTypes('toolSet/KafkaReader/KAFKA_READER_READ_DATA', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const KAFKA_READER_GET_OFFSET_RANGE = createActionTypes('toolSet/KafkaReader/KAFKA_READER_GET_OFFSET_RANGE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
