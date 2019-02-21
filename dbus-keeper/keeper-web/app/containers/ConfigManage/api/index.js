/**
 * @author xiancangao
 * @description API
 */

export const LOAD_LEVEL_OF_PATH_API = '/keeper/zookeeper/loadLevelOfPath'

export const ADD_NODE_API = '/keeper/zookeeper/addZkNodeOfPath'

export const DELETE_NODE_API = '/keeper/zookeeper/deleteZkNodeOfPath'

export const CHECK_INIT_API = '/keeper/configCenter/isInitialized'

export const READ_ZK_DATA_API = '/keeper/zookeeper/loadZKNodeJson'

export const SAVE_ZK_DATA_API = '/keeper/zookeeper/updateZKNodeJson'

export const READ_ZK_PROPERTIES_API = '/keeper/zookeeper/loadZKNodeProperties'

export const SAVE_ZK_PROPERTIES_API = '/keeper/zookeeper/updateZKNodeProperties'

export const LOAD_ZK_TREE_BY_DSNAME_API = '/keeper/zookeeper/loadZkTreeByDsName'

export const UPDATE_MGR_DB_API = '/keeper/configCenter/updateMgrDB'

export const RESET_MGR_DB_API = '/keeper/configCenter/ResetMgrDB'

export const UPDATE_GLOBAL_CONF_API = '/keeper/configCenter/updateGlobalConf'

export const INIT_GLOBAL_CONF_API = '/keeper/configCenter/updateBasicConfByOption'

export const DBA_ENCODE_SEARCH_API = '/keeper/dbaEncodeData/encodeInfo'

export const DBA_ENCODE_UPDATE_API = '/keeper/dbaEncodeData/toggleOverride'
