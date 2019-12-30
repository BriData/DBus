/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// Sink管理查询
export const SEARCH_SINK_LIST = createActionTypes('SinkManage/SEARCH_SINK_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Sink管理查询
export const CREATE_SINK = createActionTypes('SinkManage/CREATE_SINK', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Sink管理查询
export const UPDATE_SINK = createActionTypes('SinkManage/UPDATE_SINK', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// Sink管理查询
export const DELETE_SINK = createActionTypes('SinkManage/DELETE_SINK', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 设置Sink搜索参数
export const SET_SEARCH_SINK_PARAM = 'SinkManage/SET_SEARCH_SINK_PARAM'
