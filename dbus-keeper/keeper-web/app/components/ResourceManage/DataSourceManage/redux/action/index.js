/**
 * @author 戎晓伟
 * @description redux->action
 */

 // 导入创建多action的方法
import { createAction } from '@/app/utils/createAction'

// 导入用到的type
import {
  DATA_SOURCE_ALL_PARAMS,
  DATA_SOURCE_ALL_SEARCH,
  DATA_SOURCE_GET_ID_TYPE_NAME,
  DATA_SOURCE_GET_BY_ID,
  DATA_SOURCE_DELETE,
  DATA_SOURCE_UPDATE,
  DATA_SOURCE_INSERT,
  KILL_TOPOLOGY,
  DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID,
  DATA_SOURCE_GET_SCHEMA_TABLE_LIST,
  DATA_SOURCE_CLEAN_SCHEMA_TABLE,
  DATA_SOURCE_CLEAR_FULLPULL_ALARM,
  OGG_CANAL_CONF_GET_BY_DS_NAME
} from './types'

// 设置项目基本信息
// export function setBasicInfo (params) {
//   return {
//     type: PROJECT_CREATE_BASICINFO,
//     params: params
//   }
// }

// DataSource 管理查询参数
export function setDataSourceParams (params) {
  return {
    type: DATA_SOURCE_ALL_PARAMS,
    params: params
  }
}
// DataSource 消除全量报警
export const clearFullPullAlarm = {
  request: params => createAction(DATA_SOURCE_CLEAR_FULLPULL_ALARM.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_CLEAR_FULLPULL_ALARM.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_CLEAR_FULLPULL_ALARM.FAIL, { ...error })
}

// DataSource 管理查询
export const searchDataSourceList = {
  request: params => createAction(DATA_SOURCE_ALL_SEARCH.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_ALL_SEARCH.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_ALL_SEARCH.FAIL, { ...error })
}

// DataSource名称和id的列表，提供给schema级应用使用
export const searchDataSourceIdTypeName = {
  request: params => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_ID_TYPE_NAME.FAIL, { ...error })
}

// DataSource 管理查询
export const getDataSourceById = {
  request: params => createAction(DATA_SOURCE_GET_BY_ID.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_BY_ID.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_BY_ID.FAIL, { ...error })
}

// DataSource 删除
export const deleteDataSource = {
  request: params => createAction(DATA_SOURCE_DELETE.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_DELETE.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_DELETE.FAIL, { ...error })
}

// DataSource 修改
export const updateDataSource = {
  request: params => createAction(DATA_SOURCE_UPDATE.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_UPDATE.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_UPDATE.FAIL, { ...error })
}


// DataSource 修改
export const insertDataSource = {
  request: params => createAction(DATA_SOURCE_INSERT.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_INSERT.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_INSERT.FAIL, { ...error })
}

// topology kill
export const killTopology = {
  request: params => createAction(KILL_TOPOLOGY.LOAD, { ...params }),
  success: data => createAction(KILL_TOPOLOGY.SUCCESS, { ...data }),
  fail: error => createAction(KILL_TOPOLOGY.FAIL, { ...error })
}

// 获取schema列表
export const getSchemaListByDsId = {
  request: params => createAction(DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_SCHEMA_LIST_BY_DS_ID.FAIL, { ...error })
}

// 获取schema信息和table列表
export const getSchemaTableList = {
  request: params => createAction(DATA_SOURCE_GET_SCHEMA_TABLE_LIST.LOAD, { ...params }),
  success: data => createAction(DATA_SOURCE_GET_SCHEMA_TABLE_LIST.SUCCESS, { ...data }),
  fail: error => createAction(DATA_SOURCE_GET_SCHEMA_TABLE_LIST.FAIL, { ...error })
}

// DataSource 清楚schema 和 schema table数据
export function cleanSchemaTable () {
  return {
    type: DATA_SOURCE_CLEAN_SCHEMA_TABLE,
  }
}

// oggCanalConf查询
export const getOggCanalConfByDsName = {
  request: params => createAction(OGG_CANAL_CONF_GET_BY_DS_NAME.LOAD, { ...params }),
  success: data => createAction(OGG_CANAL_CONF_GET_BY_DS_NAME.SUCCESS, { ...data }),
  fail: error => createAction(OGG_CANAL_CONF_GET_BY_DS_NAME.FAIL, { ...error })
}
