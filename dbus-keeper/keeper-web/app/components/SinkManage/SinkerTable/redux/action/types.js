/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import {createActionTypes} from '@/app/utils/createAction'

// SinkerTable管理查询
export const SEARCH_SINKER_TABLE_LIST = createActionTypes('SinkManage/SEARCH_SINKER_TABLE_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// 设置SinkerTable搜索参数
export const SET_SEARCH_SINKER_TABLE_PARAM = 'SinkManage/SET_SEARCH_SINKER_TABLE_PARAM'
