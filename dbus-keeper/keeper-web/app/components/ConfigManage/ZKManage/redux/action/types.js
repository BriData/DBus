/**
 * @author 戎晓伟
 * @description redux->type
 */

// 导入创建多Type的方法
import { createActionTypes } from '@/app/utils/createAction'

// ZK查询
export const ZK_MANAGE_LOAD_LEVEL_OF_PATH = createActionTypes('configManage/zkManage/ZK_MANAGE_LOAD_LEVEL_OF_PATH', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// ZK节点读取 string
export const READ_ZK_DATA = createActionTypes('configManage/zkManage/READ_ZK_DATA', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// ZK节点保存 string
export const SAVE_ZK_DATA = createActionTypes('configManage/zkManage/SAVE_ZK_DATA', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// ZK节点读取 properties
export const READ_ZK_PROPERTIES = createActionTypes('configManage/zkManage/READ_ZK_PROPERTIES', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// ZK节点保存 properties
export const SAVE_ZK_PROPERTIES = createActionTypes('configManage/zkManage/SAVE_ZK_PROPERTIES', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])

// ZK loadZkTreeByDsName
export const LOAD_ZK_TREE_BY_DSNAME = createActionTypes('configManage/zkManage/LOAD_ZK_TREE_BY_DSNAME', [
  'LOAD',
  'SUCCESS',
  'FAIL'
])
