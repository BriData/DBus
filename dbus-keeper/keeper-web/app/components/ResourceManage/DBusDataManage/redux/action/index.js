/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_FROM_SOURCE,
  CLEAR_SOURCE
} from './types'

export const searchFromSource = {
  request: params => createAction(SEARCH_FROM_SOURCE.LOAD, { ...params }),
  success: data => createAction(SEARCH_FROM_SOURCE.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_FROM_SOURCE.FAIL, { ...error })
}

export function clearSource (params) {
  return {
    type: CLEAR_SOURCE,
    params: params
  }
}
