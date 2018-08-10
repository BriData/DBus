/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SET_ENCODE_PLUGIN_PARAM,
  SEARCH_ENCODE_PLUGIN
} from './types'

// 设置参数
export function setEncodePluginParam (params) {
  return {
    type: SET_ENCODE_PLUGIN_PARAM,
    params: params
  }
}

// 查询jar包信息
export const searchEncodePlugin = {
  request: params => createAction(SEARCH_ENCODE_PLUGIN.LOAD, { ...params }),
  success: data => createAction(SEARCH_ENCODE_PLUGIN.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_ENCODE_PLUGIN.FAIL, { ...error })
}
