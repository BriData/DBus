/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'
// 查询dbus data中的表和存储过程
export const SEARCH_FROM_SOURCE = createActionTypes('dbusData/dbusDataManager/SEARCH_FROM_SOURCE', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

export const CLEAR_SOURCE = 'dbusData/dbusDataManager/CLEAR_SOURCE'
