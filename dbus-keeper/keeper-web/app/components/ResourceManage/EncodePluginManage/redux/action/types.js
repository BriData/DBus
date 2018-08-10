/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 存储查询参数
export const SET_ENCODE_PLUGIN_PARAM = 'dataSource/encodePluginManage/SET_ENCODE_PLUGIN_PARAM'

// 更新项目
export const SEARCH_ENCODE_PLUGIN = createActionTypes('dataSource/encodePluginManage/SEARCH_ENCODE_PLUGIN', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
