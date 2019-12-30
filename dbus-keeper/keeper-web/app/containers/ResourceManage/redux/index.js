/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export EncodePluginManageReducer from '@/app/components/ResourceManage/EncodePluginManage/redux/reducer'
export JarManageReducer from '@/app/components/ResourceManage/JarManage/redux/reducer'
export DataSourceReducer from '@/app/components/ResourceManage/DataSourceManage/redux/reducer'
export DataSchemaReducer from '@/app/components/ResourceManage/DataSchemaManage/redux/reducer'
export DBusDataReducer from '@/app/components/ResourceManage/DBusDataManage/redux/reducer'
export EncodeManagerReducer from '@/app/components/ResourceManage/EncodeManager/redux/reducer'
export DataTableReducer from '@/app/components/ResourceManage/DataTableManage/redux/reducer'
export RuleGroupReducer from '@/app/components/RuleManage/RuleGroup/redux/reducer'
export DataSourceCreateReducer from '@/app/components/ResourceManage/DataSourceCreate/redux/reducer'
export Oggc from '@/app/components/ResourceManage/DataSchemaManage/redux/reducer'
/** ************导出action*******************/

// 异步请求的action
export {
  searchJarInfos
} from '@/app/components/ResourceManage/JarManage/redux/action'

// 同步存储action
// export {
//   setResourceParams
// } from '@/app/components/ProjectManage/ProjectHome/redux/action'
export {
  setDataSourceParams
} from '@/app/components/ResourceManage/DataSourceManage/redux/action'

export {
  searchDataSourceList,
  searchDataSourceIdTypeName,
  getDataSourceById,
  deleteDataSource,
  updateDataSource,
  insertDataSource,
  killTopology,
  getSchemaListByDsId,
  getSchemaTableList,
  cleanSchemaTable,
  clearFullPullAlarm,
  getOggCanalConfByDsName
} from '@/app/components/ResourceManage/DataSourceManage/redux/action'


export {
  setDataSchemaParams
} from '@/app/components/ResourceManage/DataSchemaManage/redux/action'

export {
  searchDataSchemaList,
  getDataSchemaById,
  searchAllDataSchema
} from '@/app/components/ResourceManage/DataSchemaManage/redux/action'



export {
  setDataTableParams,
  clearVersionDetail
} from '@/app/components/ResourceManage/DataTableManage/redux/action'

export {
  searchFromSource,
  clearSource
} from '@/app/components/ResourceManage/DBusDataManage/redux/action'


export {
  searchDataTableList,
  findAllDataTableList,
  getEncodeConfig,
  getTableColumn,
  getEncodeType,
  getVersionList,
  getVersionDetail,
  getSourceInsight
} from '@/app/components/ResourceManage/DataTableManage/redux/action'


export {
  searchRuleGroup
} from '@/app/components/RuleManage/RuleGroup/redux/action'

export {
  setEncodePluginParam,
  searchEncodePlugin
} from '@/app/components/ResourceManage/EncodePluginManage/redux/action'

export {
  getLatestJarPath
} from '@/app/components/ResourceManage/DataSourceCreate/redux/action'

export {
  setEncodeManagerParams
} from '@/app/components/ResourceManage/EncodeManager/redux/action'

export {
  searchEncodeList
} from '@/app/components/ResourceManage/EncodeManager/redux/action'


