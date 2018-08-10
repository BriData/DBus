/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// 更新项目
export const SEARCH_RULE_GROUP = createActionTypes('RuleManage/RuleGroup/SEARCH_RULE_GROUP', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
