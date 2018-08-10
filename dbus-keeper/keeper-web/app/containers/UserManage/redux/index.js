/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export userManageReducer from '@/app/components/UserManage/redux/reducer'

/** ************导出action*******************/

// 异步请求的action
export {
  searchUserList,
  getUserInfo,
  getUserProject
} from '@/app/components/UserManage/redux/action'

// 同步存储action
export {
  setUserListParams
} from '@/app/components/UserManage/redux/action'
