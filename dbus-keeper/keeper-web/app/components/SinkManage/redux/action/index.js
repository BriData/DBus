/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_SINK_LIST,
  CREATE_SINK,
  UPDATE_SINK,
  DELETE_SINK,
  SET_SEARCH_SINK_PARAM
} from './types'

// Sink管理查询
export const searchSinkList = {
  request: params => createAction(SEARCH_SINK_LIST.LOAD, { ...params }),
  success: data => createAction(SEARCH_SINK_LIST.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_SINK_LIST.FAIL, { ...error })
}

export const createSink = {
  request: params => createAction(CREATE_SINK.LOAD, { ...params }),
  success: data => createAction(CREATE_SINK.SUCCESS, { ...data }),
  fail: error => createAction(CREATE_SINK.FAIL, { ...error })
}

export const updateSink = {
  request: params => createAction(UPDATE_SINK.LOAD, { ...params }),
  success: data => createAction(SEARCH_SINK_LIST.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_SINK_LIST.FAIL, { ...error })
}

export const deleteSink = {
  request: params => createAction(DELETE_SINK.LOAD, { ...params }),
  success: data => createAction(DELETE_SINK.SUCCESS, { ...data }),
  fail: error => createAction(DELETE_SINK.FAIL, { ...error })
}

// 设置项目基本信息
export function setSearchSinkParam (params) {
  return {
    type: SET_SEARCH_SINK_PARAM,
    params: params
  }
}
