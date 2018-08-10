/**
 * @author xiancangao
 * @description API
 */

export const SEARCH_TABLE_DATASOURCE_LIST_API = '/keeper/data-source/getDSNames'

export const SEND_CONTROL_MESSAGE_API = '/keeper/toolSet/sendCtrlMessage'

export const READ_RELOAD_INFO_API = '/keeper/toolSet/readZKNode'

export const GLOBAL_FULL_PULL_API = '/keeper/toolSet/globalFullPull'

// 停止global topo
export const KILL_GLOBAL_FULLPULL_TOPO_API = '/keeper/toolSet/killGlobalFullPullTopo'
// 查看global topo状态
export const CHECK_GLOBAL_FULLPULL_TOPO_API = '/keeper/toolSet/getGlobalFullPullTopo'

export const KAFKA_READER_GET_TOPIC_LIST_API = '/keeper/toolSet/getTopics'

export const KAFKA_READER_READ_DATA_API = '/keeper/toolSet/kafkaReader'

export const KAFKA_READER_GET_OFFSET_RANGE_API = '/keeper/toolSet/getOffset'

