/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export projectHomeReducer from '@/app/components/ProjectManage/ProjectHome/redux/reducer'

export projectResourceReducer from '@/app/components/ProjectManage/ProjectResource/redux/reducer'

export projectTopologyReducer from '@/app/components/ProjectManage/ProjectTopology/redux/reducer'

export projectSummaryReducer from '@/app/components/ProjectManage/ProjectSummary/redux/reducer'

export projectTableReducer from '@/app/components/ProjectManage/ProjectTable/redux/reducer'

export projectFullpullReducer from '@/app/components/ProjectManage/ProjectFullpull/redux/reducer'
/** ************导出action*******************/

// 异步请求的action
export {
  searchUser,
  searchSink,
  searchResource,
  searchEncode,
  getEncodeTypeList,
  getProjectInfo
} from '@/app/components/ProjectManage/ProjectHome/redux/action'

export {
  searchAllResource,
  searchAllResourceProject,
  searchAllResourceDsName,
  searchAllResourceTableEncode
} from '@/app/components/ProjectManage/ProjectResource/redux/action'

export {
  searchAllTopology,
  getTopologyInfo,
  getTopologyJarVersions,
  getTopologyJarPackages,
  getTopologyFeeds,
  getTopologyOutTopic,
  topologyEffect,
  topologyRerunInit
} from '@/app/components/ProjectManage/ProjectTopology/redux/action'

export {
  searchProject,
  deleteProject,
  enableDisableProject
} from '@/app/components/ProjectManage/ProjectSummary/redux/action'

export {
  searchTableList,
  getProjectList,
  getTopologyList,
  getDataSourceList,
  getTableInfo,
  getResourceList,
  getColumns,
  getColumnsEncodes,
  getAllTopologyList,
  getTableSinks,
  getTableTopics,
  getTablePartitions,
  getTableAffectTables,
  sendTableReloadMsg,
  getTableProjectAllTopo
} from '@/app/components/ProjectManage/ProjectTable/redux/action'

export {
  searchAllFullpull,
  searchAllFullpullProject,
  searchAllFullpullDsName
} from '@/app/components/ProjectManage/ProjectFullpull/redux/action'

// 同步存储action
export {
  setBasicInfo,
  setUser,
  setSink,
  setResource,
  setAlarm,
  setEncodes,
  setUserParams,
  setSinkParams,
  setResourceParams
} from '@/app/components/ProjectManage/ProjectHome/redux/action'

export {
  setAllResourceParams
} from '@/app/components/ProjectManage/ProjectResource/redux/action'

export {
  setAllTopologyParams
} from '@/app/components/ProjectManage/ProjectTopology/redux/action'

export {
  setAllFullpullParams
} from '@/app/components/ProjectManage/ProjectFullpull/redux/action'

export {
  setTableParams,
  setTableResourceParams,
  setTableSink,
  setTableResource,
  setTableTopology,
  selectAllResource,
  setTableEncodes
} from '@/app/components/ProjectManage/ProjectTable/redux/action'
