/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'


export const GET_BASIC_CONF = createActionTypes('Initialization/GET_BASIC_CONF', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
