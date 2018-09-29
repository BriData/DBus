/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  UPDATE_GLOBAL_CONF,
  INIT_GLOBAL_CONF
} from './types'

export const updateGlobalConf = {
  request: params => createAction(UPDATE_GLOBAL_CONF.LOAD, { ...params }),
  success: data => createAction(UPDATE_GLOBAL_CONF.SUCCESS, { ...data }),
  fail: error => createAction(UPDATE_GLOBAL_CONF.FAIL, { ...error })
}

export const initGlobalConf = {
  request: params => createAction(INIT_GLOBAL_CONF.LOAD, { ...params }),
  success: data => createAction(INIT_GLOBAL_CONF.SUCCESS, { ...data }),
  fail: error => createAction(INIT_GLOBAL_CONF.FAIL, { ...error })
}
