/** ************导出 reducer*******************/
export sinkHomeReducer from '@/app/components/SinkManage/SinkList/redux/reducer'

/** ************导出action*******************/

// 异步请求的action
export {
  searchSinkList,
  createSink,
  updateSink,
  deleteSink
} from '@/app/components/SinkManage/SinkList/redux/action'

// 同步存储action
export {
  setSearchSinkParam
} from '@/app/components/SinkManage/SinkList/redux/action'
