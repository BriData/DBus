/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  PROJECT_FULLPULL_HISTORY_ALL_PARAMS,
  PROJECT_FULLPULL_HISTORY_ALL_SEARCH,
  PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH,
  PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH
} from './types'

// FullpullHistory管理查询参数
export function setAllFullpullParams (params) {
  return {
    type: PROJECT_FULLPULL_HISTORY_ALL_PARAMS,
    params: params
  }
}

// FullpullHistory管理查询
export const searchAllFullpull = {
  request: params => createAction(PROJECT_FULLPULL_HISTORY_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_FULLPULL_HISTORY_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_FULLPULL_HISTORY_ALL_SEARCH.FAIL, { ...error })
}

export const searchAllFullpullProject = {
  request: params => createAction(PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_FULLPULL_HISTORY_PROJECT_LIST_SEARCH.FAIL, { ...error })
}

export const searchAllFullpullDsName = {
  request: params => createAction(PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.LOAD, { ...params }),
  success: data => createAction(PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(PROJECT_FULLPULL_HISTORY_DSNAME_LIST_SEARCH.FAIL, { ...error })
}
