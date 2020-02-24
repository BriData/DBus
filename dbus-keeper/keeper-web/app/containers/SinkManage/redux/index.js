/** ************导出 reducer*******************/
export sinkHomeReducer from '@/app/components/SinkManage/SinkList/redux/reducer'
export sinkerSchemaReducer from '@/app/components/SinkManage/SinkerSchema/redux/reducer'
export sinkerTableReducer from '@/app/components/SinkManage/SinkerTable/redux/reducer'

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

// 异步请求的action
export {
  searchSinkerSchemaList
} from '@/app/components/SinkManage/SinkerSchema/redux/action'

// 同步存储action
export {
  setSearchSinkerSchemaParam
} from '@/app/components/SinkManage/SinkerSchema/redux/action'

// 异步请求的action
export {
  searchSinkerTableList
} from '@/app/components/SinkManage/SinkerTable/redux/action'

// 同步存储action
export {
  setSearchSinkerTableParam
} from '@/app/components/SinkManage/SinkerTable/redux/action'
