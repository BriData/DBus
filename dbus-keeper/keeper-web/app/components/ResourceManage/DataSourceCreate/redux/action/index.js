/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  LATEST_JAR_GET_PATH
} from './types'

// 获取schema信息和table列表
export const getLatestJarPath = {
  request: params => createAction(LATEST_JAR_GET_PATH.LOAD, { ...params }),
  success: data => createAction(LATEST_JAR_GET_PATH.SUCCESS, { ...data }),
  fail: error => createAction(LATEST_JAR_GET_PATH.FAIL, { ...error })
}
