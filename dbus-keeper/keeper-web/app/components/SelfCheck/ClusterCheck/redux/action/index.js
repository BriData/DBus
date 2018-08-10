/**
 * @author xiancangao
 * @description redux->action
 */

// 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DATA_SOURCE_LIST,
  SEND_CONTROL_MESSAGE,
  READ_RELOAD_INFO
} from './types'

// DataSource查询
export const searchDataSourceList = {
  request: params => createAction(DATA_SOURCE_LIST.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_LIST.FAIL, { ...error })
}

// send ctrl message
export const sendControlMessage = {
  request: params => createAction(SEND_CONTROL_MESSAGE.LOAD, { ...params }),
  success: data => createAction(SEND_CONTROL_MESSAGE.SUCCESS, { ...data }),
  fail: error => createAction(SEND_CONTROL_MESSAGE.FAIL, { ...error })
}

export const readReloadInfo = {
  request: params => createAction(READ_RELOAD_INFO.LOAD, { ...params }),
  success: data => createAction(READ_RELOAD_INFO.SUCCESS, { ...data }),
  fail: error => createAction(READ_RELOAD_INFO.FAIL, { ...error })
}

