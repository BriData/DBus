/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 更新项目
export const SEARCH_JAR_INFOS = createActionTypes('dataSource/jarManage/SEARCH_JAR_INFOS', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
