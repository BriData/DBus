/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 获取最新的dbus jar包位置
export const LATEST_JAR_GET_PATH = createActionTypes('dataSource/dataSourceCreate/LATEST_JAR_GET_PATH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
