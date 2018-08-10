/**
 * @author xiancangao
 * @description redux->action
 */

// 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  GLOBAL_FULL_PULL
} from './types'

export const globalFullPull = {
  request: params => createAction(GLOBAL_FULL_PULL.LOAD, { ...params }),
  success: data => createAction(GLOBAL_FULL_PULL.SUCCESS, { ...data }),
  fail: error => createAction(GLOBAL_FULL_PULL.FAIL, { ...error })
}

