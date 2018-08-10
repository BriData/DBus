/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_JAR_INFOS
} from './types'

// 设置项目基本信息
// export function setBasicInfo (params) {
//   return {
//     type: PROJECT_CREATE_BASICINFO,
//     params: params
//   }
// }

// 查询jar包信息
export const searchJarInfos = {
  request: params => createAction(SEARCH_JAR_INFOS.LOAD, { ...params }),
  success: data => createAction(SEARCH_JAR_INFOS.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_JAR_INFOS.FAIL, { ...error })
}
