/**
 * @author 戎晓伟
 * @description 搜索模块的redux  {reducer,action}
 */

/** ************导出 reducer*******************/
export initializationReducer from '@/app/components/Initialization/redux/reducer'

/** ************导出action*******************/

// 异步请求的action
export {
  getBasicConf
} from '@/app/components/Initialization/redux/action'

