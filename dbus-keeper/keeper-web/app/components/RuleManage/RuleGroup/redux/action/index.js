/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  SEARCH_RULE_GROUP
} from './types'

// 查询jar包信息
export const searchRuleGroup = {
  request: params => createAction(SEARCH_RULE_GROUP.LOAD, { ...params }),
  success: data => createAction(SEARCH_RULE_GROUP.SUCCESS, { ...data }),
  fail: error => createAction(SEARCH_RULE_GROUP.FAIL, { ...error })
}
