/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  ZK_MANAGE_LOAD_LEVEL_OF_PATH,
  READ_ZK_DATA,
  SAVE_ZK_DATA,
  READ_ZK_PROPERTIES,
  SAVE_ZK_PROPERTIES,
  LOAD_ZK_TREE_BY_DSNAME
} from './types'

export const loadLevelOfPath = {
  request: params => createAction(ZK_MANAGE_LOAD_LEVEL_OF_PATH.LOAD, { ...params }),
  success: data => createAction(ZK_MANAGE_LOAD_LEVEL_OF_PATH.SUCCESS, { ...data }),
  fail: error => createAction(ZK_MANAGE_LOAD_LEVEL_OF_PATH.FAIL, { ...error })
}

export const readZkData = {
  request: params => createAction(READ_ZK_DATA.LOAD, { ...params }),
  success: data => createAction(READ_ZK_DATA.SUCCESS, { ...data }),
  fail: error => createAction(READ_ZK_DATA.FAIL, { ...error })
}

export const saveZkData = {
  request: params => createAction(SAVE_ZK_DATA.LOAD, { ...params }),
  success: data => createAction(SAVE_ZK_DATA.SUCCESS, { ...data }),
  fail: error => createAction(SAVE_ZK_DATA.FAIL, { ...error })
}

export const readZkProperties = {
  request: params => createAction(READ_ZK_PROPERTIES.LOAD, { ...params }),
  success: data => createAction(READ_ZK_PROPERTIES.SUCCESS, { ...data }),
  fail: error => createAction(READ_ZK_PROPERTIES.FAIL, { ...error })
}

export const saveZkProperties = {
  request: params => createAction(SAVE_ZK_PROPERTIES.LOAD, { ...params }),
  success: data => createAction(SAVE_ZK_PROPERTIES.SUCCESS, { ...data }),
  fail: error => createAction(SAVE_ZK_PROPERTIES.FAIL, { ...error })
}

export const loadZkTreeByDsName = {
  request: params => createAction(LOAD_ZK_TREE_BY_DSNAME.LOAD, { ...params }),
  success: data => createAction(LOAD_ZK_TREE_BY_DSNAME.SUCCESS, { ...data }),
  fail: error => createAction(LOAD_ZK_TREE_BY_DSNAME.FAIL, { ...error })
}
