import { createActionTypes } from "@/app/utils/createAction"

/**
 * @author xiancangao
 * @description redux->type
 */
export const DATA_SOURCE_LIST = createActionTypes('toolSet/ControlMessage/DATA_SOURCE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const SEND_CONTROL_MESSAGE = createActionTypes('toolSet/ControlMessage/SEND_CONTROL_MESSAGE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const READ_RELOAD_INFO = createActionTypes('toolSet/ControlMessage/READ_RELOAD_INFO', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
