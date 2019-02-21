/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

export const DBA_ENCODE_SEARCH_PARAM = 'configCenter/dbaEncodeConfig/DBA_ENCODE_SEARCH_PARAM'
// dba脱敏查询
export const DBA_ENCODE_ALL_SEARCH = createActionTypes('configCenter/dbaEncodeConfig/DBA_ENCODE_ALL_SEARCH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
