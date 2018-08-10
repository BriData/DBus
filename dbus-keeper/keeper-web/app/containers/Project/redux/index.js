/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export projectHomeReducer from '@/app/components/ProjectManage/ProjectHome/redux/reducer'

/** ************导出action*******************/

// 异步请求的action
export {
  searchUser,
  searchSink,
  searchResource,
  getProjectInfo
} from '@/app/components/ProjectManage/ProjectHome/redux/action'

// 同步存储action
export {
  setBasicInfo,
  setUser,
  setSink,
  setResource,
  setAlarm,
  setUserParams,
  setSinkParams,
  setResourceParams
} from '@/app/components/ProjectManage/ProjectHome/redux/action'
