/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 保存
export const UPDATE_GLOBAL_CONF = createActionTypes('configManage/zkManage/UPDATE_GLOBAL_CONF', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
// 初始化
export const INIT_GLOBAL_CONF = createActionTypes('configManage/zkManage/INIT_GLOBAL_CONF', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
