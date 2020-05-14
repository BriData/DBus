// 获取sink列表
export const SEARCH_SINK_LIST_API = '/keeper/sinks/search'

// 创建sink信息
// params  {}
export const CREATE_SINK_API = '/keeper/sinks/create'

// 修改sink信息
// params id {}
export const UPDATE_SINK_API = '/keeper/sinks/update'

// 删除sink
// params id {}
export const DELETE_SINK_API = '/keeper/sinks/delete'

// sinker
export const SEARCH_SINKER_TOPOLOGY_API = '/keeper/sinker/search'
export const START_OR_STOP_TOPOLOGY_API = '/keeper/sinker/startOrStop'
export const CREATE_SINKER_TOPOLOGY_API = '/keeper/sinker/create'
export const UPDATE_SINKER_TOPOLOGY_API = '/keeper/sinker/update'
export const DELETE_SINKER_TOPOLOGY_API = '/keeper/sinker/delete'
export const RELOAD_SINKER_TOPOLOGY_API = '/keeper/sinker/reload'
export const SEARCH_SINKER_TOPOLOGY_BY_ID_API = '/keeper/sinker/searchById'
export const GET_SINKER_TOPIC_INFOS_API = '/keeper/sinker/getSinkerTopicInfos'
export const DRAG_BACK_RUN_AGAIN_API = '/keeper/sinker/dragBackRunAgain'
export const ADD_SINKER_SCHEMAS_API = '/keeper/sinker/addSinkerSchemas'
export const VIEW_LOG_API = '/keeper/sinker/view-log'

// sinkerSchema
export const SEARCH_SINKER_SCHEMA_API = '/keeper/sinkerSchema/search'
export const UPDATE_SINKER_SCHEMA_API = '/keeper/sinkerSchema/update'
export const DELETE_SINKER_SCHEMA_API = '/keeper/sinkerSchema/delete'
export const SEARCH_ALL_SINKER_SCHEMA_API = '/keeper/sinkerSchema/searchAll'
export const ADD_SINKER_TABLES_API = '/keeper/sinkerSchema/addSinkerTables'
export const BATCH_ADD_SINKER_TABLES_API = '/keeper/sinkerSchema/batchAddSinkerTables'
export const BATCH_DELETE_SINKER_SCHEMA_API = '/keeper/sinkerSchema/batchDeleteSinkerSchema'

// sinkerTable
export const SEARCH_SINKER_TABLE_API = '/keeper/sinkerTable/search'
export const SEARCH_ALL_SINKER_TABLE_API = '/keeper/sinkerTable/searchAll'
export const DELETE_SINKER_TABLE_API = '/keeper/sinkerTable/delete'
export const UPDATE_SINKER_TABLE_API = '/keeper/sinkerTable/update'
export const BATCH_DELETE_SINKER_TABLE_API = '/keeper/sinkerTable/batchDeleteSinkerTable'

