/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DBA_ENCODE_ALL_SEARCH,
  DBA_ENCODE_SEARCH_PARAM
} from './types'

export function setDbaEncodeParams (params) {
  return {
    type: DBA_ENCODE_SEARCH_PARAM,
    params: params
  }
}

export const searchDbaEncodeList = {
  request: params => createAction(DBA_ENCODE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DBA_ENCODE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DBA_ENCODE_ALL_SEARCH.FAIL, { ...error })
}
