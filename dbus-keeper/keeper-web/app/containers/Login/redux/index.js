/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export loginReducer from '@/app/components/Login/redux/reducer'

/** ************导出action*******************/

// 异步请求的action
export {
    loginList
  } from '@/app/components/Login/redux/action'

  // 同步存储action
export {
    setParams
  } from '@/app/components/Login/redux/action'
