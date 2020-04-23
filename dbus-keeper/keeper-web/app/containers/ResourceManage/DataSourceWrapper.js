import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
import {message, Modal} from 'antd'
// 导入自定义组件
import {
  Bread,
  CanalConfModifyModal,
  DataSourceManageAddModal,
  DataSourceManageGrid,
  DataSourceManageModifyModal,
  DataSourceManageMountModal,
  DataSourceManageRerunModal,
  DataSourceManageSearch,
  DataSourceManageTopologyModal,
  GenerateAddTableSqlModal,
  OggConfModifyModal,
  SearchDatasourceExistModal
} from '@/app/components'
// selectors
import {DataSourceModel, JarManageModel} from './selectors'
import {ZKManageModel} from '../ConfigManage/selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {
  cleanSchemaTable,
  clearFullPullAlarm,
  getDataSourceById,
  getOggCanalConfByDsName,
  getSchemaListByDsId,
  getSchemaTableList,
  killTopology,
  searchDataSourceList,
  searchJarInfos,
  setDataSourceParams
} from './redux'
import {readZkData, saveZkData} from '../ConfigManage/redux'
import {
  DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API,
  DATA_SOURCE_DELETE_API,
  DATA_SOURCE_GENERATE_OGG_TRAIN_NAME_API,
  DATA_SOURCE_SEARCH_DATASOURCE_EXIST_API,
  DATA_SOURCE_UPDATE_API,
  DOWNLOAD_PRE_PROCESS_MODEL_API,
  TOPO_JAR_START_API,
  INIT_CANAL_FILTER_API
} from './api'
import {GET_MOUNT_PROJECT_API} from '../ProjectManage/api'
import Request from "@/app/utils/request";
// 链接reducer和action
@connect(
  createStructuredSelector({
    JarManageData: JarManageModel(),
    dataSourceData: DataSourceModel(),
    ZKManageData: ZKManageModel(),
    locale: makeSelectLocale(),
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
    getOggCanalConfByDsName: param => dispatch(getOggCanalConfByDsName.request(param)),
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

      generateAddTableSqlKey: 'generateAddTableSqlKey',
      generateAddTableSqlVisible: false,

      oggModifyModalKey: 'oggModifyModalKey',
      oggModifyModalVisible: false,

      canalModifyModalKey: 'canalModifyModalKey',
      canalModifyModalVisible: false,

      searchDatasourceExistKey: 'searchDatasourceExistKey',
      searchDatasourceExistVisible: false
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
    if (boolean || boolean === undefined) {
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
    window.location.href = '/resource-manage/datasource-create'
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

  handleOpenOggModifyModal = record => {
    const {getOggCanalConfByDsName} = this.props
    getOggCanalConfByDsName(record)
    this.setState({
      oggModifyModalVisible: true,
    })
  }

  handleCloseOggModify = () => {
    this.setState({
      oggModifyModalKey: this.handleRandom('oggModifyModalKey'),
      oggModifyModalVisible: false,
    })
    this.handleRefresh()
  }

  handleOpenCanalModifyModal = record => {
    const {getOggCanalConfByDsName} = this.props
    getOggCanalConfByDsName(record)
    this.setState({
      canalModifyModalVisible: true,
    })
  }

  handleCloseCanalModify = () => {
    this.setState({
      canalModifyModalKey: this.handleRandom('canalModifyModalKey'),
      canalModifyModalVisible: false,
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

  handleInitCanalFilter = record => {
    Request(INIT_CANAL_FILTER_API, {
      params: {
        dsId: record.id,
        dsName: record.name
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
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
      method: 'get'
    })
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

  handleOpenGenerateAddTableSqlModal = () => {
    this.setState({
      generateAddTableSqlKey: this.handleRandom('generateAddTableSqlKey'),
      generateAddTableSqlVisible: true
    })
  }

  handleCloseGenerateAddTableSqlModal = () => {
    this.setState({
      generateAddTableSqlVisible: false
    })
  }

  handleGenerateOggTrailName = () => {
    Request(DATA_SOURCE_GENERATE_OGG_TRAIN_NAME_API)
      .then(res => {
        if (res && res.status === 0) {
          Modal.info({
            content: <span style={{fontSize: 14}}>OGG Trail前缀：{res.payload}</span>
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleCloseSearchDatasourceExist = () => {
    this.setState({
      searchDatasourceExistKey: this.handleRandom('searchDatasourceExistKey'),
      searchDatasourceExistVisible: false
    })
    this.handleRefresh()
  }

  handleOpensearchDatasourceExist = () => {
    this.setState({
      searchDatasourceExistVisible: true
    })
  }

  handleSearchDatasourceExist = (param) => {
    Request(DATA_SOURCE_SEARCH_DATASOURCE_EXIST_API, {
      params: {
        ip: param.ip,
        port: param.port
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          Modal.info({
            content: <span style={{fontSize: 14}}>接入数据源：{res.payload}</span>
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleDownload = () => {
    const TOKEN = window.localStorage.getItem('TOKEN')
    window.open(`${DOWNLOAD_PRE_PROCESS_MODEL_API}?token=${TOKEN}`)
  }

  render() {
    // console.info(this.props)
    const {locale, dataSourceData} = this.props

    const {dataSourceParams, dataSourceList} = dataSourceData

    const {theDataSourceGottenById} = dataSourceData
    const {oggCanalConfDsName} = dataSourceData

    const {modifyModalKey, modifyModalVisible} = this.state

    const {oggModifyModalKey, oggModifyModalVisible, canalModifyModalKey, canalModifyModalVisible} = this.state

    const {topoModalKey, topoModalId, topoModalVisible} = this.state

    const {addModalKey, addModalVisible, addModalRecord} = this.state
    const {getSchemaTableList} = this.props
    const schemaList = dataSourceData.schemaList.result.payload || []
    const schemaTableResult = dataSourceData.schemaTableResult

    const jarInfos = this.props.JarManageData.jarInfos.result.payload || []
    const {searchJarInfos} = this.props

    const {readZkData, saveZkData} = this.props
    const zkData = this.props.ZKManageData.zkData.result.payload || {}

    const {mountModalContent, mountModalVisible, mountModalKey} = this.state
    const {rerunModalVisible, rerunModalRecord, rerunModalKey} = this.state
    const {generateAddTableSqlKey, generateAddTableSqlVisible} = this.state
    const {searchDatasourceExistKey, searchDatasourceExistVisible} = this.state
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
          onGenerateAddTableSql={this.handleOpenGenerateAddTableSqlModal}
          onGenerateOggTrailName={this.handleGenerateOggTrailName}
          onOpenSearchDatasourceExist={this.handleOpensearchDatasourceExist}
          onDownload={this.handleDownload}
        />
        <DataSourceManageGrid
          dataSourceList={dataSourceList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onModify={this.handleOpenModifyModal}
          onOggModify={this.handleOpenOggModifyModal}
          onCanalModify={this.handleOpenCanalModifyModal}
          onTopo={this.handleOpenTopoModal}
          onAdd={this.handleOpenAddModal}
          onMount={this.handleMount}
          onDBusData={this.handleDBusData}
          onClearFullPullAlarm={this.handleClearFullPullAlarm}
          deleteApi={DATA_SOURCE_DELETE_API}
          onRefresh={this.handleRefresh}
          onRerun={this.handleOpenRerunModal}
          onInitCanalFilter={this.handleInitCanalFilter}
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
        <GenerateAddTableSqlModal
          key={generateAddTableSqlKey}
          visible={generateAddTableSqlVisible}
          onClose={this.handleCloseGenerateAddTableSqlModal}
        />
        <OggConfModifyModal
          key={oggModifyModalKey}
          visible={oggModifyModalVisible}
          onClose={this.handleCloseOggModify}
          oggCanalConf={oggCanalConfDsName}
        />
        <CanalConfModifyModal
          key={canalModifyModalKey}
          visible={canalModifyModalVisible}
          onClose={this.handleCloseCanalModify}
          oggCanalConf={oggCanalConfDsName}
        />
        <SearchDatasourceExistModal
          key={searchDatasourceExistKey}
          visible={searchDatasourceExistVisible}
          onClose={this.handleCloseSearchDatasourceExist}
          onSearch={this.handleSearchDatasourceExist}
        />
      </div>
    )
  }
}
DataSourceWrapper.propTypes = {
  locale: PropTypes.any
}
