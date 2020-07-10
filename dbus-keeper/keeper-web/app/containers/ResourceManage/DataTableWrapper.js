import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Request from '@/app/utils/request'
import Helmet from 'react-helmet'
import {message} from 'antd'
// 导入自定义组件
import {
  Bread,
  DataSourceManageCheckModal,
  DataSourceManageMountModal,
  DataTableBatchFullPullModal,
  DataTableManageEncodeModal,
  DataTableManageGrid,
  DataTableManageIndependentModal,
  DataTableManageModifyModal,
  DataTableManageReadZkModal,
  DataTableManageRerunModal,
  DataTableManageSearch,
  DataTableManageSourceInsightModal,
  DataTableManageVersionModal,
  DataTableMoveModal,
  RuleImportModal
} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {DataSourceModel, DataTableModel} from './selectors'
import {ZKManageModel} from "@/app/containers/ConfigManage/selectors";

import {
  clearVersionDetail,
  getEncodeConfig,
  getEncodeType,
  getSourceInsight,
  getTableColumn,
  getVersionDetail,
  getVersionList,
  searchDataSourceIdTypeName,
  searchDataTableList,
  setDataTableParams
} from './redux'
import {sendControlMessage,} from '@/app/containers/toolSet/redux'
import {loadLevelOfPath, readZkData} from "@/app/components/ConfigManage/ZKManage/redux/action";

import {
  DATA_SOURCE_FIND_SCHEMA_LIST_BY_DS_ID_API,
  DATA_TABLE_BATCH_DELETE_API,
  DATA_TABLE_BATCH_START_API,
  DATA_TABLE_BATCH_STOP_API,
  DATA_TABLE_CHECK_DATA_LINE_API,
  DATA_TABLE_DELETE_API,
  DATA_TABLE_MOVE_TABLES_API,
  DATA_TABLE_SAVE_ENCODE_CONFIG_API,
  DATA_TABLE_START_API,
  DATA_TABLE_STOP_API,
  DATA_TABLE_UPDATE_API,
  EXPORT_RULES_API,
  REINIT_TABLE_META_API
} from './api'
import {GET_MOUNT_PROJECT_API, PROJECT_TABLE_BATCH_FULLPULL_API} from "@/app/containers/ProjectManage/api";


// 链接reducer和action
@connect(
  createStructuredSelector({
    dataSourceData: DataSourceModel(),
    dataTableData: DataTableModel(),
    ZKManageData: ZKManageModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchDataSourceIdTypeName: param => dispatch(searchDataSourceIdTypeName.request(param)),
    clearVersionDetail: param => dispatch(clearVersionDetail(param)),
    setDataTableParams: param => dispatch(setDataTableParams(param)),
    searchDataTableList: param => dispatch(searchDataTableList.request(param)),
    getEncodeConfig: param => dispatch(getEncodeConfig.request(param)),
    getTableColumn: param => dispatch(getTableColumn.request(param)),
    getEncodeType: param => dispatch(getEncodeType.request(param)),
    getVersionList: param => dispatch(getVersionList.request(param)),
    getVersionDetail: param => dispatch(getVersionDetail.request(param)),
    getSourceInsight: param => dispatch(getSourceInsight.request(param)),

    loadLevelOfPath: param => dispatch(loadLevelOfPath.request(param)),
    readZkData: param => dispatch(readZkData.request(param)),

    sendControlMessage: param => dispatch(sendControlMessage.request(param)),
  })
)
export default class DataTableWrapper extends Component {
  constructor(props) {
    super(props)
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
    this.state = {
      modifyModalKey: 'modifyModalKey',
      modifyModalVisible: false,
      modifyModalRecord: {},

      zkModalKey: 'zkModalKey',
      zkModalVisible: false,
      zkModalRecord: {},

      versionModalKey: 'versionKey',
      versionModalVisible: false,
      versionModalRecord: {},

      sourceInsightModalKey: 'sourceInsight',
      sourceInsightModalVisible: false,
      sourceInsightModalRecord: {},

      independentModalKey: 'independentModalKey',
      independentModalVisible: false,
      independentModalRecord: {},
      independentModalLoading: false,

      mountModalKey: 'mountModalKey',
      mountModalVisible: false,
      mountModalContent: [],

      checkModalKey: 'checkModalKey',
      checkModalVisible: false,
      checkModalResult: {},
      checkModalLoading: false,

      selectedRowKeys: [],
      selectedRows: [],

      rerunModalKey: 'rerunModalKey',
      rerunModalRecord: {},
      rerunModalVisible: false,

      ruleImportModalKey: 'ruleImportModalKey',
      ruleImportModalVisible: false,
      ruleImportModalTableId: 1,

      batchFullPullModalVisible: false,
      batchFullPullModalKey: 'batchFullPull',

      moveTablesModalVisible: false,
      moveTablesModalKey: 'moveTables',

      schemaList: [{id: null, schemaName: '请选择schema'}]
    }
  }

  componentWillMount() {
    // 初始化查询
    const {searchDataSourceIdTypeName} = this.props
    searchDataSourceIdTypeName()
    this.handleSearch(this.initParams)
  }

  handleSendControlMessage = data => {
    const {sendControlMessage} = this.props
    sendControlMessage(data)
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleSearch = (params, boolean) => {
    const {searchDataTableList, setDataTableParams} = this.props
    searchDataTableList(params)
    if (boolean || boolean === undefined) {
      setDataTableParams(params)
    }
    this.setState({
      selectedRowKeys: [],
      selectedRows: []
    })
  }

  handlePagination = page => {
    const {dataTableData} = this.props
    const {dataTableParams} = dataTableData
    // 分页查询并存储参数
    this.handleSearch({...dataTableParams, pageNum: page})
  }

  handleShowSizeChange = (current, size) => {
    const {dataTableData} = this.props
    const {dataTableParams} = dataTableData
    // 分页查询并存储参数
    this.handleSearch({...dataTableParams, pageNum: current, pageSize: size})
  }

  handleMount = record => {
    Request(GET_MOUNT_PROJECT_API, {
      params: {
        tableId: record.id
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

  handleSelectionChange = (selectedRowKeys, selectedRows) => {
    this.setState({selectedRowKeys, selectedRows})
  }

  handleOpenEncodeModal = record => {
    const {getEncodeConfig, getTableColumn, getEncodeType} = this.props
    getEncodeConfig(record)
    getTableColumn(record)
    getEncodeType()
    this.setState({
      encodeModalVisible: true,
      encodeModalRecord: record
    })
  }

  handleCloseEncodeModal = () => {
    this.setState({
      encodeModalKey: this.handleRandom('encode'),
      encodeModalVisible: false
    })
  }

  handleOpenModifyModal = record => {
    this.setState({
      modifyModalVisible: true,
      modifyModalRecord: record
    })
  }

  handleCloseModify = () => {
    this.setState({
      modifyModalKey: this.handleRandom('modify'),
      modifyModalVisible: false
    })
  }

  handleOpenVersionModal = record => {
    const {getVersionList} = this.props
    getVersionList(record)
    this.setState({
      versionModalKey: this.handleRandom('version'),
      versionModalVisible: true,
      versionModalRecord: record
    })
  }

  handleCloseVersionModal = () => {
    const {clearVersionDetail} = this.props
    clearVersionDetail()
    this.setState({
      versionModalKey: this.handleRandom('version'),
      versionModalVisible: false
    })
  }


  handleOpenSourceInsightModal = record => {
    const {getSourceInsight} = this.props
    getSourceInsight({
      tableId: record.id,
      number: 10
    })
    this.setState({
      sourceInsightModalVisible: true,
      sourceInsightModalRecord: record
    })
  }

  handleCloseSourceInsightModal = () => {
    this.setState({
      sourceInsightModalKey: this.handleRandom('sourceInsight'),
      sourceInsightModalVisible: false
    })
  }

  handleOpenCheckDataLineModal = record => {
    this.setState({
      checkModalKey: this.handleRandom('checkModalKey'),
      checkModalVisible: true,
      checkModalResult: {},
      checkModalLoading: true,
    })
    Request(`${DATA_TABLE_CHECK_DATA_LINE_API}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        this.setState({
          checkModalLoading: false,
        })
        if (res && res.status === 0) {
          if (!res.payload.status) {
            message.error('检查程序出错')
            return
          }
          this.setState({
            checkModalResult: res.payload,
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        this.setState({
          checkModalLoading: false,
        })
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleCloseCheckDataLineModal = () => {
    this.setState({
      checkModalKey: this.handleRandom('checkModalKey'),
      checkModalVisible: false
    })
  }

  handleOpenIndependentFullPullModal = record => {
    this.setState({
      independentModalKey: this.handleRandom('independentModalKey'),
      independentModalRecord: record,
      independentModalVisible: true,
    })
  }

  handleIndependentLoading = () => {
    this.setState({independentModalLoading: true})
  }

  handleCloseIndependentFullPullModal = () => {
    this.setState({
      independentModalVisible: false,
      independentModalLoading: false
    })
    this.handleRefresh()
  }

  handleRule = record => {
    this.props.router.push({
      pathname: '/resource-manage/rule-group',
      query: {
        tableId: record.id,
        dsId: record.dsId,
        dsName: record.dsName,
        dsType: record.dsType,
        schemaName: record.schemaName,
        tableName: record.tableName,
        ctrlTopic: record.ctrlTopic,
      }
    })
  }

  handleReadZk = path => {
    const {readZkData} = this.props
    readZkData({path})
  }

  handleOpenZKModal = record => {
    const {loadLevelOfPath} = this.props
    loadLevelOfPath({
      path: `/DBus/FullPuller/${record.dsName}/${record.schemaName}/${record.tableName}`
    })
    this.setState({
      zkModalVisible: true,
      zkModalRecord: record
    })
  }

  handleCloseZKModal = () => {
    this.setState({
      zkModalKey: this.handleRandom('zk'),
      zkModalVisible: false,
    })
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

  handleRefresh = () => {
    const {dataTableData} = this.props
    const {dataTableParams} = dataTableData
    this.handleSearch({...dataTableParams}, false)
  }

  handleRequest = (obj) => {
    const {api, params, data, method, callback, callbackParams} = obj
    Request(api, {
      params: {
        ...params
      },
      data: {
        ...data
      },
      method: method
    })
      .then(res => {
        if (res && res.status === 0) {
          if (callback) {
            if (callbackParams) callback(...callbackParams)
            else callback()
          }
          this.handleRefresh()
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

  handleAllStart = () => {
    const {selectedRowKeys} = this.state
    if (!selectedRowKeys.length) {
      message.error('没有选择表')
      return
    }
    Request(`${DATA_TABLE_BATCH_START_API}`, {
      data: selectedRowKeys,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleRefresh()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleAllStop = () => {
    // debugger
    const {selectedRowKeys} = this.state
    if (!selectedRowKeys.length) {
      message.error('没有选择表')
      return
    }
    Request(`${DATA_TABLE_BATCH_STOP_API}`, {
      data: selectedRowKeys,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleRefresh()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleAllDelete = () => {
    // debugger
    const {selectedRowKeys, selectedRows} = this.state
    if (!selectedRowKeys.length) {
      message.error('没有选择表')
      return
    }
    for (let v of selectedRows) {
      if (v.status === 'ok') {
        message.error('runing状态的表不允许删除')
        return
      }
    }
    Request(`${DATA_TABLE_BATCH_DELETE_API}`, {
      data: selectedRowKeys,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.setState({
            selectedRows: [],
            selectedRowKeys: []
          })
          this.handleRefresh()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  handleCloseRuleImportModal = () => {
    this.setState({
      ruleImportModalVisible: false
    })
  }

  handleOpenRuleImportModal = (record) => {
    this.setState({
      ruleImportModalKey: this.handleRandom('preProcessModalKey'),
      ruleImportModalVisible: true,
      ruleImportModalTableId: record.id
    })
  }

  handleDownload = (record) => {
    const TOKEN = window.localStorage.getItem('TOKEN')
    window.open(`${EXPORT_RULES_API}/${record.id}?token=${TOKEN}`)
  }

  handleOpenBatchFullPullModal = () => {
    const {selectedRows} = this.state
    if (!selectedRows.length) {
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
    if (!selectedRows.length) {
      message.error('没有选择表')
      return
    }
    Request(`${PROJECT_TABLE_BATCH_FULLPULL_API}`, {
      data: {
        outputTopic: values.topic,
        hdfsRootPath: values.hdfsRootPath,
        isProject: false,
        ids: selectedRows.map(row => row.id)
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
          this.handleRefresh()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }

  getSchemaListByDsId = (dsId) => {
    Request(`${DATA_SOURCE_FIND_SCHEMA_LIST_BY_DS_ID_API}`, {
      params: {
        dsId: dsId,
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({schemaList: res.payload})
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))

  }

  handleOpenMoveTablesModal = () => {
    const {dataTableData} = this.props
    const {dataTableParams} = dataTableData
    const {dsId, schemaId} = dataTableParams

    if (!dsId || !schemaId) {
      message.error('请选择具体数据源和schema并点击查询!')
      return
    }
    const {selectedRows} = this.state
    if (!selectedRows.length) {
      message.error('请选择需要迁移的表!')
      return
    }
    this.setState({
      moveTablesModalVisible: true,
      moveTablesModalKey: this.handleRandom('moveTables')
    })
  }

  handleCloseMoveTablesModal = () => {
    this.setState({
      moveTablesModalVisible: false
    })
  }


  handleMoveTable = (values) => {
    const {selectedRows} = this.state
    Request(`${DATA_TABLE_MOVE_TABLES_API}`, {
      data: {
        dsId: values.dsId,
        tableIds: selectedRows.map(row => row.id)
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.handleCloseMoveTablesModal()
          this.handleRefresh()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => message.error(error))
  }


  render() {
    const {dataSourceData} = this.props
    const {dataSourceIdTypeName} = dataSourceData

    const {dataTableData} = this.props
    const {dataTableParams, dataTableList} = dataTableData

    const {modifyModalKey, modifyModalVisible, modifyModalRecord} = this.state
    const {schemaList} = this.state

    const {encodeModalKey, encodeModalVisible, encodeModalRecord} = this.state
    const {encodeConfigList, tableColumnList, encodeTypeList} = dataTableData

    const {zkModalKey, zkModalVisible, zkModalRecord} = this.state
    const {ZKManageData} = this.props
    const {levelOfPath, zkData} = ZKManageData

    const {versionModalKey, versionModalVisible, versionModalRecord} = this.state
    const versionLoading = dataTableData.versionDetail.loading
    const versionList = dataTableData.versionList.result.payload
    const versionDetail = dataTableData.versionDetail.result.payload
    const {getVersionDetail} = this.props


    const {sourceInsightModalVisible, sourceInsightModalKey, sourceInsightModalRecord} = this.state
    const {getSourceInsight} = this.props
    const sourceInsightResult = dataTableData.sourceInsightResult

    const {independentModalVisible, independentModalRecord, independentModalKey, independentModalLoading} = this.state

    const {mountModalContent, mountModalVisible, mountModalKey} = this.state
    const {selectedRowKeys} = this.state

    const {checkModalKey, checkModalLoading, checkModalResult, checkModalVisible} = this.state
    const {rerunModalVisible, rerunModalRecord, rerunModalKey} = this.state
    const {ruleImportModalKey, ruleImportModalVisible, ruleImportModalTableId} = this.state
    const {batchFullPullModalVisible, batchFullPullModalKey} = this.state
    const {moveTablesModalVisible, moveTablesModalKey} = this.state

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
        path: '/resource-manage/data-table',
        name: 'DataTable管理'
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
        <DataTableManageSearch
          dataSourceIdTypeName={dataSourceIdTypeName}
          schemaList={schemaList}
          params={dataTableParams}
          onSearch={this.handleSearch}
          onGetSchemaList={this.getSchemaListByDsId}
          selectedRowKeys={selectedRowKeys}
          onAllStart={this.handleAllStart}
          onAllStop={this.handleAllStop}
          onSendControlMessage={this.handleSendControlMessage}
          onBatchFullPull={this.handleOpenBatchFullPullModal}
          onMoveTables={this.handleOpenMoveTablesModal}
          onAllDelete={this.handleAllDelete}
        />
        <DataTableManageGrid
          selectedRowKeys={selectedRowKeys}
          dataTableList={dataTableList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onModify={this.handleOpenModifyModal}
          onOpenEncodeModal={this.handleOpenEncodeModal}
          onOpenZKModal={this.handleOpenZKModal}
          onOpenVersionModal={this.handleOpenVersionModal}
          onOpenSourceInsightModal={this.handleOpenSourceInsightModal}
          onOpenIndependentFullPullModal={this.handleOpenIndependentFullPullModal}
          onCheckDataLine={this.handleOpenCheckDataLineModal}
          onRule={this.handleRule}
          onMount={this.handleMount}
          onSelectionChange={this.handleSelectionChange}
          onRequest={this.handleRequest}
          updateApi={DATA_TABLE_UPDATE_API}
          deleteApi={DATA_TABLE_DELETE_API}
          startApi={DATA_TABLE_START_API}
          reInitMeta={REINIT_TABLE_META_API}
          stopApi={DATA_TABLE_STOP_API}
          onSendControlMessage={this.handleSendControlMessage}
          onRerun={this.handleOpenRerunModal}
          onOpenRuleImportModal={this.handleOpenRuleImportModal}
          onHandleDownload={this.handleDownload}
        />
        <DataTableManageReadZkModal
          key={zkModalKey}
          visible={zkModalVisible}
          record={zkModalRecord}
          onClose={this.handleCloseZKModal}
          node={levelOfPath}
          zkData={zkData}
          onReadZk={this.handleReadZk}
        />
        <DataTableManageEncodeModal
          key={encodeModalKey}
          visible={encodeModalVisible}
          tableInfo={encodeModalRecord}
          encodeConfigList={encodeConfigList}
          tableColumnList={tableColumnList}
          encodeTypeList={encodeTypeList}
          onRequest={this.handleRequest}
          onClose={this.handleCloseEncodeModal}
          saveApi={DATA_TABLE_SAVE_ENCODE_CONFIG_API}
        />
        <DataTableManageModifyModal
          key={modifyModalKey}
          visible={modifyModalVisible}
          tableInfo={modifyModalRecord}
          onRequest={this.handleRequest}
          updateApi={DATA_TABLE_UPDATE_API}
          onClose={this.handleCloseModify}
        />
        <DataTableManageVersionModal
          key={versionModalKey}
          visible={versionModalVisible}
          tableInfo={versionModalRecord}
          versionList={versionList}
          versionDetail={versionDetail}
          loading={versionLoading}
          getVersionDetail={getVersionDetail}
          onClose={this.handleCloseVersionModal}
        />
        <DataTableManageSourceInsightModal
          key={sourceInsightModalKey}
          visible={sourceInsightModalVisible}
          tableInfo={sourceInsightModalRecord}
          getSourceInsight={getSourceInsight}
          sourceInsightResult={sourceInsightResult}
          onClose={this.handleCloseSourceInsightModal}
        />
        <DataTableManageIndependentModal
          key={independentModalKey}
          visible={independentModalVisible}
          record={independentModalRecord}
          onRequest={this.handleRequest}
          onClose={this.handleCloseIndependentFullPullModal}
          loading={independentModalLoading}
          onLoading={this.handleIndependentLoading}
        />
        <DataSourceManageMountModal
          key={mountModalKey}
          visible={mountModalVisible}
          content={mountModalContent}
          onClose={this.handleCloseMountModal}
        />
        <DataSourceManageCheckModal
          key={checkModalKey}
          loading={checkModalLoading}
          result={checkModalResult}
          visible={checkModalVisible}
          onClose={this.handleCloseCheckDataLineModal}
        />
        <DataTableManageRerunModal
          key={rerunModalKey}
          visible={rerunModalVisible}
          record={rerunModalRecord}
          onClose={this.handleCloseRerunModal}
        />
        <RuleImportModal
          key={ruleImportModalKey}
          visible={ruleImportModalVisible}
          tableId={ruleImportModalTableId}
          onClose={this.handleCloseRuleImportModal}
        />
        <DataTableBatchFullPullModal
          key={batchFullPullModalKey}
          visible={batchFullPullModalVisible}
          onClose={this.handleCloseBatchFullPullModal}
          onRequest={this.handleRequest}
          onBatchFullPull={this.handleBatchFullPull}
        />
        <DataTableMoveModal
          key={moveTablesModalKey}
          visible={moveTablesModalVisible}
          onClose={this.handleCloseMoveTablesModal}
          onMoveTable={this.handleMoveTable}
          dataSourceIdTypeName={dataSourceIdTypeName}
        />

      </div>
    )
  }
}
DataTableWrapper.propTypes = {
  locale: PropTypes.any,
}
