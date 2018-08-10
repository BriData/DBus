/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

export const ENCODE_MANAGER_SET_PARAM = 'ResourceManage/EncodeManager/ENCODE_MANAGER_SET_PARAM'

export const ENCODE_MANAGER_SEARCH = createActionTypes('ResourceManage/EncodeManager/ENCODE_MANAGER_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
