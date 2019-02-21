import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {message, Modal} from 'antd'
// 导入自定义组件
import {
  Bread,
  DataSourceManageSearch,
  DataSourceManageGrid,
  DataSourceManageModifyModal,
  DataSourceManageTopologyModal,
  DataSourceManageMountModal,
  DataSourceManageAddModal,
  DataSourceManageRerunModal,
  DataSourceManageBatchAddTableModal,
  DataSourceManagePreProcessModal
} from '@/app/components'
// selectors
import {DataSourceModel,JarManageModel} from './selectors'
import {ZKManageModel} from '../ConfigManage/selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {
  setDataSourceParams,
  searchDataSourceList,
  getDataSourceById,
  killTopology,
  getSchemaListByDsId,
  getSchemaTableList,
  cleanSchemaTable,
  searchJarInfos,
  clearFullPullAlarm
} from './redux'
import {
  readZkData,
  saveZkData
} from '../ConfigManage/redux'
import {
  DATA_SOURCE_UPDATE_API,
  DATA_SOURCE_DELETE_API,
  DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API,
  TOPO_JAR_START_API,
  DATA_SOURCE_GENERATE_OGG_TRAIN_NAME_API
} from './api'
import {
  GET_MOUNT_PROJECT_API
} from '../ProjectManage/api'
import Request from "@/app/utils/request";

// 链接reducer和action
@connect(
  createStructuredSelector({
    JarManageData: JarManageModel(),
    dataSourceData: DataSourceModel(),
    ZKManageData: ZKManageModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    clearFullPullAlarm: param => dispatch(clearFullPullAlarm.request(param)),
    setDataSourceParams: param => dispatch(setDataSourceParams(param)),
    searchDataSourceList: param => dispatch(searchDataSourceList.request(param)),
    getDataSourceById: param => dispatch(getDataSourceById.request(param)),
    killTopology: param => dispatch(killTopology.request(param)),
    getSchemaListByDsId: param => dispatch(getSchemaListByDsId.request(param)),
    getSchemaTableList: param => dispatch(getSchemaTableList.request(param)),
    cleanSchemaTable: param => dispatch(cleanSchemaTable(param)),
    searchJarInfos: param => dispatch(searchJarInfos.request(param)),
    readZkData: param => dispatch(readZkData.request(param)),
    saveZkData: param => dispatch(saveZkData.request(param)),
  })
)
export default class DataSourceWrapper extends Component {
  constructor(props) {
    super(props)
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
    this.state = {
      modifyModalKey: 'modifyModalKey',
      modifyModalVisible: false,

      topoModalKey: 'topoModalKey',
      topoModalVisible: false,
      topoModalId: -1,

      addModalKey: 'addModalKey',
      addModalVisible: false,
      addModalRecord: {},

      mountModalKey: 'mountModalKey',
      mountModalVisible: false,
      mountModalContent: [],

      rerunModalKey: 'rerunModalKey',
      rerunModalVisible: false,
      rerunModalRecord: {},

      batchAddTableModalKey: 'batchAddTableModalKey',
      batchAddTableModalVisible: false,

      preProcessModalKey: 'preProcessModalKey',
      preProcessModalVisible: false
    }
  }

  componentWillMount() {
    // 初始化查询
    this.handleSearch(this.initParams)
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  /**
   * @description 查询datasource列表
   */
  handleSearch = (params, boolean) => {
    const {searchDataSourceList, setDataSourceParams} = this.props
    searchDataSourceList(params)
    if(boolean || boolean === undefined) {
      setDataSourceParams(params)
    }
  }

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const {dataSourceData} = this.props
    const {dataSourceParams} = dataSourceData
    // 分页查询并存储参数
    this.handleSearch({...dataSourceParams, pageNum: page})
  }

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const {dataSourceData} = this.props
    const {dataSourceParams} = dataSourceData
    // 分页查询并存储参数
    this.handleSearch({...dataSourceParams, pageNum: current, pageSize: size})
  }

  handleCreateDataSource = () => {
    window.location.href='/resource-manage/datasource-create'
  }

  handleClearFullPullAlarm = record => {
    const {clearFullPullAlarm} = this.props
    clearFullPullAlarm({
      dsName: record.name
    })
  }

  handleOpenModifyModal = record => {
    const {getDataSourceById} = this.props
    getDataSourceById(record)
    this.setState({
      modifyModalVisible: true
    })
  }

  handleCloseModify = () => {
    this.setState({
      modifyModalKey: this.handleRandom('modify'),
      modifyModalVisible: false
    })
    this.handleRefresh()
  }

  handleOpenTopoModal = id => {
    this.setState({
      topoModalVisible: true,
      topoModalId: id
    })
  }

  /**
   * @params {topologyId, waitTime}
   */
  handleKillTopology = params => {
    const {killTopology} = this.props
    killTopology(params)
  }

  handleCloseTopoModal = record => {
    this.setState({
      topoModalKey: this.handleRandom('topo'),
      topoModalVisible: false,
    })
  }

  handleOpenAddModal = record => {
    const {getSchemaListByDsId, cleanSchemaTable} = this.props
    getSchemaListByDsId({dsId: record.id})
    cleanSchemaTable()
    this.setState({
      addModalVisible: true,
      addModalRecord: record
    })
  }

  handleCloseAddModal = () => {
    this.setState({
      addModalKey: this.handleRandom('addModalKey'),
      addModalVisible: false,
    })
  }

  handleMount = record => {
    Request(GET_MOUNT_PROJECT_API, {
      params: {
        dsId: record.id
      },
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.handleOpenMountModal(res.payload)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleOpenMountModal = content => {
    this.setState({
      mountModalKey: this.handleRandom('mountModalKey'),
      mountModalVisible: true,
      mountModalContent: content
    })
  }

  handleCloseMountModal = () => {
    this.setState({
      mountModalKey: this.handleRandom('mountModalKey'),
      mountModalVisible: false
    })
  }

  handleDBusData = record => {
    this.props.router.push({
      pathname: '/resource-manage/dbus-data',
      query: {
        dsId: record.id
      }
    })
  }

  handleRefresh = () => {
    const {dataSourceData} = this.props
    const {dataSourceParams} = dataSourceData
    this.handleSearch({...dataSourceParams})
  }

  handleOpenRerunModal = record => {
    this.setState({
      rerunModalKey: this.handleRandom('rerunModalKey'),
      rerunModalRecord: record,
      rerunModalVisible: true
    })
  }

  handleCloseRerunModal = () => {
    this.setState({
      rerunModalVisible: false
    })
  }

  handleOpenBatchAddTableModal = () => {
    this.setState({
      batchAddTableModalKey: this.handleRandom('batchAddTableModalKey'),
      batchAddTableModalVisible: true
    })
  }

  handleCloseBatchAddTableModal = () => {
    this.setState({
      batchAddTableModalVisible: false
    })
  }

  handleOpenPreProcessModal = () => {
    this.setState({
      preProcessModalKey: this.handleRandom('preProcessModalKey'),
      preProcessModalVisible: true
    })
  }

  handleClosePreProcessModal = () => {
    this.setState({
      preProcessModalVisible: false
    })
  }

  handleGenerateOggTrailName = () => {
    Request(DATA_SOURCE_GENERATE_OGG_TRAIN_NAME_API)
      .then(res => {
        if (res && res.status === 0) {
          Modal.info({
            content: <span style={{fontSize: 14 }}>OGG Trail前缀：{res.payload}</span>,
          });
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  render() {
    console.info(this.props)
    const {locale, dataSourceData} = this.props
    const {dataSourceParams, dataSourceList} = dataSourceData

    const {theDataSourceGottenById} = dataSourceData
    const {modifyModalKey, modifyModalVisible} = this.state

    const {topoModalKey, topoModalId, topoModalVisible} = this.state

    const {addModalKey, addModalVisible, addModalRecord} = this.state
    const {getSchemaTableList} = this.props
    const schemaList = dataSourceData.schemaList.result.payload || []
    const schemaTableResult = dataSourceData.schemaTableResult

    const jarInfos = this.props.JarManageData.jarInfos.result.payload || []
    const {searchJarInfos} = this.props

    const {readZkData,saveZkData} = this.props
    const zkData = this.props.ZKManageData.zkData.result.payload || {}

    const {mountModalContent, mountModalVisible, mountModalKey} = this.state
    const {rerunModalVisible, rerunModalRecord, rerunModalKey} = this.state
    const {batchAddTableModalKey, batchAddTableModalVisible} = this.state
    const {preProcessModalKey, preProcessModalVisible} = this.state
    const breadSource = [
      {
        path: '/resource-manage',
        name: 'home'
      },
      {
        path: '/resource-manage',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/data-source',
        name: 'DataSource管理'
      }
    ]
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSource Manage'}
          ]}
        />
        <Bread source={breadSource}/>
        <DataSourceManageSearch
          params={dataSourceParams}
          onSearch={this.handleSearch}
          onCreateDataSource={this.handleCreateDataSource}
          onBatchAddTable={this.handleOpenBatchAddTableModal}
          onPreProcess={this.handleOpenPreProcessModal}
          onGenerateOggTrailName={this.handleGenerateOggTrailName}
        />
        <DataSourceManageGrid
          dataSourceList={dataSourceList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onModify={this.handleOpenModifyModal}
          onTopo={this.handleOpenTopoModal}
          onAdd={this.handleOpenAddModal}
          onMount={this.handleMount}
          onDBusData={this.handleDBusData}
          onClearFullPullAlarm={this.handleClearFullPullAlarm}
          deleteApi={DATA_SOURCE_DELETE_API}
          onRefresh={this.handleRefresh}
          onRerun={this.handleOpenRerunModal}
        />
        <DataSourceManageModifyModal
          key={modifyModalKey}
          visible={modifyModalVisible}
          dsInfo={theDataSourceGottenById}
          onClose={this.handleCloseModify}
          updateApi={DATA_SOURCE_UPDATE_API}
        />
        <DataSourceManageTopologyModal
          key={topoModalKey}
          visible={topoModalVisible}
          id={topoModalId}
          dataSourceList={dataSourceList}
          onRefresh={this.handleRefresh}
          onClose={this.handleCloseTopoModal}
          onKill={this.handleKillTopology}
          jarInfos={jarInfos}
          searchJarInfos={searchJarInfos}
          topoJarStartApi={TOPO_JAR_START_API}
          readZkData={readZkData}
          saveZkData={saveZkData}
          zkData={zkData}
        />
        <DataSourceManageMountModal
          key={mountModalKey}
          visible={mountModalVisible}
          content={mountModalContent}
          onClose={this.handleCloseMountModal}
        />
        <DataSourceManageAddModal
          key={addModalKey}
          visible={addModalVisible}
          record={addModalRecord}
          onClose={this.handleCloseAddModal}
          schemaList={schemaList}
          schemaTableResult={schemaTableResult}
          getSchemaTableList={getSchemaTableList}
          addApi={DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API}
        />
        <DataSourceManageRerunModal
          key={rerunModalKey}
          visible={rerunModalVisible}
          record={rerunModalRecord}
          onClose={this.handleCloseRerunModal}
        />
        <DataSourceManageBatchAddTableModal
          key={batchAddTableModalKey}
          visible={batchAddTableModalVisible}
          onClose={this.handleCloseBatchAddTableModal}
        />
        <DataSourceManagePreProcessModal
          key={preProcessModalKey}
          visible={preProcessModalVisible}
          onClose={this.handleClosePreProcessModal}
        />
      </div>
    )
  }
}
DataSourceWrapper.propTypes = {
  locale: PropTypes.any,
}
