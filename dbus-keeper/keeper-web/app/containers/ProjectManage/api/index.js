/**
 * @author 戎晓伟
 * @description API
 */
// 禁用启用项目
// params {}
export const ENABLE_DISABLE_PROJECT_API = '/keeper/projects/status'

// 删除
// params {}
export const DELETE_PROJECT_API = '/keeper/projects/delete'

// 获取项目信息
// params {}
export const GET_PROJECT_INFO_API = '/keeper/projects'
// 项目列表查询
// params {}
export const SEARCH_PROJECT_API = '/keeper/projects'

// 新增项目
// params {}
export const ADD_PROJECT_API = '/keeper/projects/add'

// 修改项目
// params {}
export const MODIFY_PROJECT_API = '/keeper/projects/update'

// 用户查询
// params {pageNum,pageSize,sortby,order,userName,email,phoneNum}
export const SEARCH_USER_API = '/keeper/projects/users'

// Sink查询
// params {pageNum,pageSize,sortby,order,url,sinkName}
export const SEARCH_SINK_API = '/keeper/projects/sinks'

// Resource查询
// params {String dsName,String schemaName,String tableName,int pageNum,int pageSize,String sortby,String order}
export const SEARCH_RESOURCE_API = '/keeper/projects/resources'
// 查看已挂载项目 参数 Integer dsId, Integer tableId, Integer sinkId 任传其一
export const GET_MOUNT_PROJECT_API = '/keeper/projects/getMountedProjrct'



// 项目管理-Admin身份资源管理Resource查询
export const RESOURCE_MANAGE_AS_ADMIN_API = '/keeper/projectResource/resources'
// 项目管理-User身份资源管理Resource查询
export const RESOURCE_MANAGE_AS_USER_API = '/keeper/projectResource/project-resource'
// 项目管理-查询脱敏
export const RESOURCE_MANAGE_TABLE_ENCODE_API = '/keeper/projectResource/mask'
// 项目管理-资源管理Resource project查询
export const RESOURCE_MANAGE_PROJECT_API = '/keeper/projectResource/project-names'
// 项目管理-资源管理Resource dsName查询
export const RESOURCE_MANAGE_DSNAME_API = '/keeper/projectResource/dsNames'

// 脱敏配置查询
// params {tableId}
export const SEARCH_ENCODE_API = '/keeper/projects/columns'

// 脱敏配置-脱敏规则-下拉列表
// params {tableId}
export const SEARCH_ENCODE_SELECT_API = '/keeper/projectTable/encoders'

// ProjectTable

// projectTable table查询
// params {dsName,schemaName,tableName,pageNum,pageSize,projectId,topoId}
export const SEARCH_TABLE_LIST_API = '/keeper/projectTable/tables'

// projectTable topology-names
// params {projectId}
export const SEARCH_TABLE_TOPOLOGY_LIST_API =
  '/keeper/projectTable/topology-names'

// projectTable project-names
// params {}
export const SEARCH_TABLE_PROJECT_LIST_API =
  '/keeper/projectTable/project-names'

// projectTable datasource-names
// params {}
export const SEARCH_TABLE_DATASOURCE_LIST_API =
  '/keeper/projectTable/datasource-names'

// projectTable project-resources
// params {schemaName,dsName,tableName,pageNum,pageSize,projectId}
export const SEARCH_TABLE_RESOURCES_LIST_API =
  '/keeper/projectTable/project-resources'

// projectTable columns
// params {tableId}
export const SEARCH_TABLE_RESOURCES_COLUMNS_LIST_API =
  '/keeper/projectTable/columns'

// projectTable 新建获取Sink
// params {projectId}
export const GET_TABLE_SINK_API = '/keeper/projectTable/sinks'

// projectTable 新建获取Topic
// params {sinkId}
export const GET_TABLE_TOPIC_API = '/keeper/projectTable/topics'

// projectTable 新建获取project所有topo
// params {projectId}
export const GET_TABLE_PROJECT_TOPO_API = '/keeper/projectTable/project-topologies'

// 获取项目下所有resource
export const GET_PROJECT_ALL_RESOURCE = '/keeper/projectTable/getAllResourcesByQuery'
// 获取所有resource
export const GET_ALL_RESOURCE = '/keeper/projects/getAllResourcesByQuery'

// projectTable 新增Table
// params {}
export const ADD_TABLE_API = '/keeper/projectTable/add'

// projectTable 修改Table
// params {projectId}
export const MODIFY_TABLE_API = '/keeper/projectTable/update'

// projectTable 获取table信息
// params {id}
export const GET_TABLE_INFO_API = '/keeper/projectTable'

// 根据topic 获取partitions
export const GET_TABLE_PARTITION_API = '/keeper/projectTable/partitions'

// 根据topic,tableId 获取受影响的表
export const GET_TABLE_AFFECT_TABLE_API = '/keeper/projectTable/affected-tables'

// 修改offset
// [{tableId:1,topic:'top',partition:'0',offset:'head/latest/1234或者为"" '},
// {tableId:1,topic:'top',partition:'0',offset:'head/latest/1234或者为"" '}]
export const START_TABLE_PARTITION_OFFSET_API = '/keeper/projectTable/start'
// 停止table
export const STOP_TABLE_PARTITION_OFFSET_API = '/keeper/projectTable/stop'

// Reload Table
export const RELOAD_TABLE_API = '/keeper/projectTable/reload'

// 批量拉全量
export const PROJECT_TABLE_BATCH_FULLPULL_API = '/keeper/fullpull/batchGlobalfullPull'

// 删除table
export const PROJECT_TABLE_DELETE_API = '/keeper/projectTable/delete'

// 批量停止
export const PROJECT_TABLE_BATCH_STOP_API = '/keeper/projectTable/batchStop'

// 批量启动
export const PROJECT_TABLE_BATCH_START_API = '/keeper/projectTable/batchStart'

// 获取拉全量配置
export const PROJECT_TABLE_GET_INITIAL_LOAD_CONF_API = '/keeper/projectTable/getProjectTableById'
// 保存拉全量配置
export const PROJECT_TABLE_SAVE_INITIAL_LOAD_CONF_API = '/keeper/fullpull/updateCondition'
// 拉全量
export const PROJECT_TABLE_INITIAL_LOAD_API = '/keeper/projectTable/initialLoad'


// projectTable startTable
// projectTable stopTable
// projectTable  delectTable
// projectTable  effective
// projectTable  abate
// projectTable  fullPull

// projectTopology

// projectTopology 查询Topology
// params {projectId,topoName,pageNum,pageSize,sortby,order}
export const SEARCH_TOPOLOGY_LIST_API = '/keeper/project-topos/topos'

// projectTopology 获取Topo信息
// params {topoId}
export const GET_TOPOLOGY_INFO_API = '/keeper/project-topos/select'

// projectTopology 新增Topology
// params {topoName,topoConfig,jarVersion,jarFilePath,status,topoComment,projectId}
export const ADD_TOPOLOGY_API = '/keeper/project-topos/insert'

// projectTopology 修改Topology
// params {topoName,topoConfig,jarVersion,jarFilePath,status,topoComment,projectId}
export const MODIFY_TOPOLOGY_API = '/keeper/project-topos/update'

// projectTopology 获取Jar版本
// params {}
export const SEARCH_TOPOLOGY_JAR_VERSIONS_API =
  '/keeper/project-topos/versions'

// projectTopology 获取Jar包
// params {version}
export const SEARCH_TOPOLOGY_JAR_PACKAGES_API =
  '/keeper/project-topos/packages'

// projectTopology 判断topoName是否存在
// params {topoName}
export const IS_TOPOLOGY_TOPONAME_TRUE_API = '/keeper/project-topos/exist'

export const DELETE_TOPOLOGY_API = '/keeper/project-topos/delete'

export const GET_TOPOLOGY_TEMPLATE_API = '/keeper/project-topos/template'

export const TOPOLOGY_EFFECT_API = '/keeper/project-topos/effect'

export const TOPOLOGY_RERUN_INIT_API = '/keeper/project-topos/rerun-init'

export const TOPOLOGY_RERUN_API = '/keeper/project-topos/rerun'

// export const START_OR_STOP_TOPOLOGY_API = `ws://${window.location.host}/keeper-webSocket/project-topos-opt`
export const START_OR_STOP_TOPOLOGY_API = `/keeper/project-topos/operate`

// Topology 获取订阅源
export const PROJECT_TOPOLOGY_FEEDS_SEARCH_API = '/keeper/project-topos/in-topics'
// Topology 获取输出topic列表
export const PROJECT_TOPOLOGY_OUTTOPIC_SEARCH_API = '/keeper/project-topos/out-topics'

// projectFullPull 全量拉取历史记录查询
// params {dsName,schemaName,tableName,pageNum,pageSize,projectId}
export const FULLPULL_HISTORY_LIST_API = '/keeper/fullpullHistory/search'
//  projectFullPull 全量拉取历史记录查询 project查询
export const FULLPULL_HISTORY_LIST_PROJECT_API = '/keeper/fullpullHistory/project-names'
//  projectFullPull 全量拉取历史记录查询 dsName查询
export const FULLPULL_HISTORY_LIST_DSNAME_API = '/keeper/fullpullHistory/dsNames'
//  projectFullPull 修改
export const FULLPULL_HISTORY_UPDATE_API = '/keeper/fullpullHistory/update'
