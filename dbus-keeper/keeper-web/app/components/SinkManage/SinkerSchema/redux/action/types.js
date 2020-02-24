/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import {createActionTypes} from '@/app/utils/createAction'

// SinkerSchema管理查询
export const SEARCH_SINKER_SCHEMA_LIST = createActionTypes('SinkManage/SEARCH_SINKER_SCHEMA_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 设置SinkerSchema搜索参数
export const SET_SEARCH_SINKER_SCHEMA_PARAM = 'SinkManage/SET_SEARCH_SINKER_SCHEMA_PARAM'
