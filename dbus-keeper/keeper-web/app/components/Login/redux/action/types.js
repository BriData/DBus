/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 模拟本地存储
export const LOGIN_SAVE_PARAMS = 'Login/LOGIN_SAVE_PARAMS'

// 模拟请求
export const LOGIN_GET_LIST = createActionTypes('Login/LOGIN_GET_LIST', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
