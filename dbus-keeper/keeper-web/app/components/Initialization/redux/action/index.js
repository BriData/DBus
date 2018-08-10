/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  GET_BASIC_CONF
} from './types'

// 模拟本地存储 action
// export function setParams (params) {
//   return {
//     type: LOGIN_SAVE_PARAMS,
//     params: params
//   }
// }

// 模拟请求 action
export const getBasicConf = {
  request: params => createAction(GET_BASIC_CONF.LOAD, { ...params }),
  success: data => createAction(GET_BASIC_CONF.SUCCESS, { ...data }),
  fail: error => createAction(GET_BASIC_CONF.FAIL, { ...error })
}

