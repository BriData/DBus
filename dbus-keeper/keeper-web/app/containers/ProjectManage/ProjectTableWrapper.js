import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import { message } from 'antd'
// 导入自定义组件
import {
  ProjectTableGrid,
  ProjectTableSearch,
  AddProjectTable,
  ProjectTableStartModal,
  ProjectTableInitialLoadModal,
  ProjectTableKafkaReaderModal,
  ProjectTableBatchFullPullModal,
  Bread
} from '@/app/components'
// selectors
import { ProjectTableModel } from './selectors'
import { ProjectHomeModel } from '@/app/containers/ProjectManage/selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  searchTableList,
  getProjectList,
  getTopologyList,
  getDataSourceList,
  getTableInfo,
  getResourceList,
  getColumns,
  setTableParams,
  setTableResourceParams,
  setTableSink,
  setTableResource,
  setTableTopology,
  selectAllResource,
  setTableEncodes,
  getEncodeTypeList,
  getTableSinks,
  getTableTopics,
  getTablePartitions,
  getTableAffectTables,
  sendTableReloadMsg,
  getTableProjectAllTopo,
  getProjectInfo
} from './redux'

import {
  START_TABLE_PARTITION_OFFSET_API,
  STOP_TABLE_PARTITION_OFFSET_API,
  PROJECT_TABLE_DELETE_API,
  PROJECT_TABLE_INITIAL_LOAD_API,
  PROJECT_TABLE_BATCH_STOP_API,
  PROJECT_TABLE_BATCH_START_API,
  PROJECT_TABLE_BATCH_FULLPULL_API
} from './api'
import Request from '@/app/utils/request'
import {KafkaReaderModel} from "@/app/containers/toolSet/selectors";
import {getTopicsByUserId, readKafkaData} from "@/app/containers/toolSet/redux";

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectTableData: ProjectTableModel(),
    projectHomeData: ProjectHomeModel(),
    KafkaReaderData: KafkaReaderModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    getTopicsByUserId: param => dispatch(getTopicsByUserId.request(param)),
    readKafkaData: param => dispatch(readKafkaData.request(param)),
    getTableList: param => dispatch(searchTableList.request(param)),
    getProjectList: param => dispatch(getProjectList.request(param)),
    getTopologyList: param => dispatch(getTopologyList.request(param)),
    getDataSourceList: param => dispatch(getDataSourceList.request(param)),
    getTableInfo: param => dispatch(getTableInfo.request(param)),
    getResourceList: param => dispatch(getResourceList.request(param)),
    getColumns: param => dispatch(getColumns.request(param)),
    getEncodeTypeList: param => dispatch(getEncodeTypeList.request(param)),
    getTableSinks: param => dispatch(getTableSinks.request(param)),
    getTableTopics: param => dispatch(getTableTopics.request(param)),
    getTablePartitions: param => dispatch(getTablePartitions.request(param)),
    getTableAffectTables: param => dispatch(getTableAffectTables.request(param)),
    sendTableReloadMsg: param => dispatch(sendTableReloadMsg.request(param)),
    getTableProjectAllTopo: param => dispatch(getTableProjectAllTopo.request(param)),
    getProjectInfo: param => dispatch(getProjectInfo.request(param)),
    setTableParams: param => dispatch(setTableParams(param)),
    setResourceParams: param => dispatch(setTableResourceParams(param)),
    setTableSink: param => dispatch(setTableSink(param)),
    setTableResource: param => dispatch(setTableResource(param)),
    setTableTopology: param => dispatch(setTableTopology(param)),
    selectAllResource: param => dispatch(selectAllResource(param)),
    setTableEncodes: param => dispatch(setTableEncodes(param))
  })
)
export default class ProjectTableWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      modalVisibal: false,
      modalStatus: 'create',
      modalKey: '00000000000000000000000',
      modifyRecord: null,

      startModalVisible: false,
      startModalKey: '00000000000000000000000startModal',
      startModalRecord: null,
      tableId: null,

      initialLoadModalVisible: false,
      initialLoadModalKey: 'initialLoad',
      initialLoadModalRecord: {},

      kafkaReadModalVisible: false,
      kafkaReadModalKey: 'kafkaRead',
      kafkaReadModalRecord: {},

      batchFullPullModalVisible: false,
      batchFullPullModalKey: 'batchFullPull',

      selectedRowKeys: [],
      selectedRows: []
    }
    this.tableWidth = [
      '10%',
      '10%',
      '11%',
      '14%',
      '10%',
      '22%',
      '22%',
      '9%',
      '10%',
      '10%',
      '7%',
      '8%',
      '9%',
      '200px'
    ]
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
    this.projectId = null
  }
  componentWillMount () {
    const {
      getProjectList,
      getDataSourceList,
      getTopologyList,
      projectId
    } = this.props
    this.projectId = projectId
    // 初始化查询，有projectId，身份为User
    projectId && (this.initParams.projectId = projectId, this.initParams.isUser = true)
    this.handleSearch({...this.initParams, projectId: this.projectId}, true)
    // 获取project
    !this.projectId && getProjectList()
    // 获取 DataSource
    getDataSourceList({projectId})
    // 获取TopoList
    getTopologyList({projectId: this.projectId || null})
  }
  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params, boolean) => {
    const { getTableList, setTableParams } = this.props
    // 查询
    getTableList(params)
    if (boolean) {
      // 存储
      setTableParams(params)
    }
  };

  /**
   * 新建和编辑弹窗是否显示
   */

  handleCreateTable = () => {
    if(!this.projectId) {
      message.error('请选择项目')
      return
    }
    this.stateModalVisibal(true, 'create')
  }

  handleBatchStop = () => {
    const {selectedRows} = this.state
    if(!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    Request(`${PROJECT_TABLE_BATCH_STOP_API}`, {
      data: selectedRows.map(row => row.tableId),
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleReloadSearch()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleBatchStart = () => {
    const {selectedRows} = this.state
    if(!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    Request(`${PROJECT_TABLE_BATCH_START_API}`, {
      data: selectedRows.map(row => row.tableId),
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleReloadSearch()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleBatchDelete = () => {
    const {selectedRows} = this.state
    if(!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    Request(`${PROJECT_TABLE_DELETE_API}`, {
      data: selectedRows.map(row => row.tableId),
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleReloadSearch()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  stateModalVisibal = (modalVisibal, modalStatus = 'create') => {
    this.setState({
      modalVisibal,
      modalStatus,
      modalKey: this.handleRandom('modal')
    })
  };
  /**
   *@description 修改Table
   */
  handleModifyTable = (id, projectId, modifyRecord) => {
    const { getTableInfo } = this.props
    getTableInfo({ id, projectId })
    this.projectId = projectId
    this.setState({tableId: id, modifyRecord})
    this.stateModalVisibal(true, 'modify')
  };
  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const { projectTableData } = this.props
    const { tableParams } = projectTableData
    // 分页查询并存储参数
    this.handleSearch({ ...tableParams, pageNum: page }, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const { projectTableData } = this.props
    const { tableParams } = projectTableData
    this.handleSearch(
      { ...tableParams, pageNum: current, pageSize: size },
      true
    )
  };

  handleSelectionChange = (selectedRowKeys, selectedRows) => {
    this.setState({selectedRowKeys, selectedRows})
  }
  /**
   * @description 重新刷新页面的 查询接口
  */
  handleReloadSearch=() => {
    const { projectTableData, getDataSourceList, getTopologyList } = this.props
    const { tableParams } = projectTableData
    this.handleSearch(tableParams, false)
    getDataSourceList({projectId: this.projectId})
    getTopologyList({projectId: this.projectId || null})
  }

  stateStartModalVisible = startModalVisible => {
    this.setState({ startModalVisible })
    if (startModalVisible === false) {
      this.setState({ startModalKey: this.handleRandom('startModal') })
      this.setState({ startModalRecord: null })
    }
  };

  handleStart = record => {
    this.stateStartModalVisible(true)
    const { getTablePartitions, getTableAffectTables } = this.props
    getTablePartitions({ topic: record.inputTopic })
    getTableAffectTables({ topic: record.inputTopic, tableId: record.tableId })
    this.setState({ startModalRecord: record})
  }

  handleDelete = record => {
    console.info(record)
    Request(`${PROJECT_TABLE_DELETE_API}/${record.tableId}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.handleReloadSearch()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleReload = record => {
    const {sendTableReloadMsg} = this.props
    sendTableReloadMsg({tableId: record.tableId, topoName: record.topoName})
  }

  handleStartModalOk = (partition, values) => {
    const {projectTableData, getTableList} = this.props
    const {tableParams} = projectTableData

    const {startModalRecord} = this.state
    const {tableId, topoName} = startModalRecord
    const data = partition.map(p => ({
      tableId: tableId,
      topic: p.topic,
      topoName: topoName,
      partition: p.partition,
      offset: values[`offset-${p.partition}`],
      projectName: startModalRecord.projectName,
      dsName: startModalRecord.dsName,
      schemaName: startModalRecord.schemaName,
      tableName: startModalRecord.tableName,
    }))
    Request(START_TABLE_PARTITION_OFFSET_API, {
      data: data,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          message.success('5秒后会自动刷新一次')
          setTimeout(() => {
            getTableList(tableParams)
          }, 5000)
          this.stateStartModalVisible(false)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleStop = record => {
    const {projectTableData, getTableList} = this.props
    const {tableParams} = projectTableData

    Request(STOP_TABLE_PARTITION_OFFSET_API, {
      params: {
        tableId: record.tableId,
        topoName: record.topoName
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          message.success('5秒后会自动刷新一次')
          setTimeout(() => {
            getTableList(tableParams)
          }, 5000)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleOpenInitialLoadModal = (record) => {
    this.setState({
      initialLoadModalKey: this.handleRandom('initialLoad'),
      initialLoadModalVisible: true,
      initialLoadModalRecord: record
    })
  }

  handleCloseInitialLoadModal = () => {
    this.setState({
      initialLoadModalVisible: false,
      initialLoadModalKey: this.handleRandom('initialLoad')
    })
  }

  handleOpenBatchFullPullModal = () => {
    const {selectedRows} = this.state
    if(!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    this.setState({
      batchFullPullModalVisible: this.handleRandom('batchFullPull'),
      batchFullPullModalKey: true
    })
  }

  handleCloseBatchFullPullModal = () => {
    this.setState({
      batchFullPullModalVisible: false,
      batchFullPullModalKey: this.handleRandom('batchFullPull')
    })
  }

  handleBatchFullPull = (values) => {
    const {selectedRows} = this.state
    if(!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    Request(`${PROJECT_TABLE_BATCH_FULLPULL_API}`, {
      data: {
        outputTopic: values.topic,
        isProject: true,
        ids: selectedRows.map(row => row.tableId)
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success('请查看全量历史,查询批量拉全量情况!')
          this.handleCloseBatchFullPullModal()
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleReloadSearch()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }
  handleRequest = (obj) => {
    const {projectTableData} = this.props
    const {tableParams} = projectTableData

    const {api, params, data, method, callback, callbackParams} = obj
    Request(api, {
      params: {
        ...params
      },
      data: {
        ...data
      },
      method: method || 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          if (callback) {
            if (callbackParams) callback(...callbackParams)
            else callback()
          }
          this.handleSearch(tableParams)
          message.success(res.message)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleViewFullPullHistory = (record) => {
    const {onViewFullPullHistory} = this.props
    if (onViewFullPullHistory) {
      onViewFullPullHistory(record)
    }
    else this.props.router.push({
      pathname: '/project-manage/fullpull-history',
      query: {
        projectName: record.projectName,
        projectDisplayName: record.projectDisplayName,
        dsName: record.dsName,
        schemaName: record.schemaName,
        tableName: record.tableName
      }
    })
  }

  handleOpenReadKafkaModal = record => {
    this.setState({
      kafkaReadModalKey: this.handleRandom('kafkaRead'),
      kafkaReadModalVisible: true,
      kafkaReadModalRecord: record
    })
  }

  handleCloseReadKafkaModal = () => {
    this.setState({
      kafkaReadModalVisible: false,
    })
  }

  render () {
    const { modalVisibal, modalStatus, modalKey, tableId, modifyRecord} = this.state
    const { startModalVisible, startModalKey } = this.state
    const { kafkaReadModalRecord, kafkaReadModalVisible, kafkaReadModalKey } = this.state

    const {initialLoadModalVisible, initialLoadModalKey,initialLoadModalRecord} = this.state
    const {batchFullPullModalVisible, batchFullPullModalKey} = this.state
    const {selectedRowKeys} = this.state
    const {
      locale,
      // isCreate 用来判断是否是用户角度进入此组件
      isCreate,
      KafkaReaderData,
      projectTableData,
      projectHomeData,
      getTopologyList,
      getResourceList,
      getEncodeTypeList,
      getColumns,
      getTableSinks,
      getTableTopics,
      setResourceParams,
      setTableSink,
      setTableResource,
      setTableTopology,
      selectAllResource,
      setTableEncodes,
      getTableProjectAllTopo,
      getDataSourceList,
      getProjectInfo,
      getTopicsByUserId,
      readKafkaData
    } = this.props
    console.info('KafkaReaderData=',KafkaReaderData)
    const {
      topicsByUserIdList,
      kafkaData
    } = KafkaReaderData
    const {
      tableList,
      dataSourceList,
      topologyList,
      projectList,
      tableInfo,
      tableParams,
      projectTableStorage,
      partitionList,
      affectTableList,
    } = projectTableData
    const { encodeTypeList,projectInfo } = projectHomeData
    const breadSource = [
      {
        path: '/project-manage',
        name: 'home'
      },
      {
        path: '/project-manage',
        name: '项目管理'
      },
      {
        name: 'Table'
      }
    ]
    this.props.projectDisplayName && breadSource.push({
      name: this.props.projectDisplayName
    })
    return (
      <div>
        <Helmet
          title="ProjectTable"
          meta={[
            { name: 'description', content: 'Description of ProjectTable' }
          ]}
        />
        <Bread source={breadSource} />
        <ProjectTableSearch
          locale={locale}
          isCreate={isCreate}
          projectId={this.projectId}
          onSetProjectId={id => this.projectId = id}
          tableParams={tableParams}
          projectList={projectList}
          dataSourceList={dataSourceList}
          topologyList={topologyList}
          onGetTopologyList={getTopologyList}
          onSearch={this.handleSearch}
          onCreateTable={this.handleCreateTable}
          onBatchDelete={this.handleBatchDelete}
          onBatchStop={this.handleBatchStop}
          onBatchStart={this.handleBatchStart}
          onBatchFullPull={this.handleOpenBatchFullPullModal}
        />
        <ProjectTableGrid
          locale={locale}
          tableWidth={this.tableWidth}
          tableList={tableList}
          tableParams={tableParams}
          onSearch={this.handleSearch}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onOpenReadKafkaModal={this.handleOpenReadKafkaModal}
          onStart={this.handleStart}
          onStop={this.handleStop}
          onDelete={this.handleDelete}
          onReload={this.handleReload}
          onModifyTable={this.handleModifyTable}
          onInitialLoad={this.handleOpenInitialLoadModal}
          onViewFullPullHistory={this.handleViewFullPullHistory}
          onSelectionChange={this.handleSelectionChange}
          selectedRowKeys={selectedRowKeys}
        />
        <AddProjectTable
          locale={locale}
          modalVisibal={modalVisibal}
          modalStatus={modalStatus}
          modalKey={modalKey}
          modifyRecord={modifyRecord}
          projectId={this.projectId}
          tableId={tableId}
          onCloseModal={this.stateModalVisibal}
          projectTableData={projectTableData}
          encodeTypeList={encodeTypeList}
          onGetEncodeTypeList={getEncodeTypeList}
          onGetResourceList={getResourceList}
          onGetColumns={getColumns}
          onGetTableSinks={getTableSinks}
          onGetTableTopics={getTableTopics}
          onSetResourceParams={setResourceParams}
          onSetSink={setTableSink}
          onSetResource={setTableResource}
          onSetTopology={setTableTopology}
          onSelectAllResource={selectAllResource}
          onSetEncodes={setTableEncodes}
          onGetTableProjectAllTopo={getTableProjectAllTopo}
          onReloadSearch={this.handleReloadSearch}
          onReload={this.handleReload}
          getProjectInfo={getProjectInfo}
          projectInfo={projectInfo}
        />
        <ProjectTableStartModal
          modalKey={startModalKey}
          visible={startModalVisible}
          onCancel={this.stateStartModalVisible}
          onOk={this.handleStartModalOk}
          partitionList={partitionList}
          affectTableList={affectTableList}
        />
        <ProjectTableInitialLoadModal
          key={initialLoadModalKey}
          visible={initialLoadModalVisible}
          record={initialLoadModalRecord}
          onClose={this.handleCloseInitialLoadModal}
          onRequest={this.handleRequest}
          initialLoadApi={PROJECT_TABLE_INITIAL_LOAD_API}
        />
        <ProjectTableKafkaReaderModal
          key={kafkaReadModalKey}
          visible={kafkaReadModalVisible}
          record={kafkaReadModalRecord}
          onClose={this.handleCloseReadKafkaModal}
          getTopicsByUserId={getTopicsByUserId}
          readKafkaData={readKafkaData}
          topicsByUserIdList={topicsByUserIdList}
          kafkaData={kafkaData}
        />
        <ProjectTableBatchFullPullModal
          key={batchFullPullModalKey}
          visible={batchFullPullModalVisible}
          onClose={this.handleCloseBatchFullPullModal}
          onRequest={this.handleRequest}
          onBatchFullPull={this.handleBatchFullPull}
        />
      </div>
    )
  }
}
ProjectTableWrapper.propTypes = {
  locale: PropTypes.any,
  projectTableData: PropTypes.object,
  projectHomeData: PropTypes.object,
  getTableList: PropTypes.func,
  getProjectList: PropTypes.func,
  getTopologyList: PropTypes.func,
  getDataSourceList: PropTypes.func,
  getTableInfo: PropTypes.func,
  getResourceList: PropTypes.func,
  getColumns: PropTypes.func,
  getEncodeTypeList: PropTypes.func,
  setTableParams: PropTypes.func,
  setResourceParams: PropTypes.func,
  setTableSink: PropTypes.func,
  setTableResource: PropTypes.func,
  setTableTopology: PropTypes.func,
  setTableEncodes: PropTypes.func,
  getTableSinks: PropTypes.func,
  getTableTopics: PropTypes.func,
  getTableProjectAllTopo: PropTypes.func
}
