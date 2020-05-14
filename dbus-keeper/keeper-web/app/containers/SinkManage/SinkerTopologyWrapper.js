import React, {Component} from 'react'
import {connect} from 'react-redux'
import {message} from 'antd'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {
  Bread,
  ProjectTopologyStartModal,
  SinkerTopologyAddSchemaModal,
  SinkerTopologyForm,
  SinkerTopologyManageGrid,
  SinkerTopologyManageSearch,
  SinkerTopologyRerunModal
} from '@/app/components/index'
// selectors
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {intlMessage} from '@/app/i18n'
import Request from '@/app/utils/request'
import {
  DRAG_BACK_RUN_AGAIN_API,
  GET_SINKER_TOPIC_INFOS_API,
  SEARCH_ALL_SINKER_SCHEMA_API,
  SEARCH_SINKER_TOPOLOGY_API,
  SEARCH_SINKER_TOPOLOGY_BY_ID_API,
  START_OR_STOP_TOPOLOGY_API,
  VIEW_LOG_API
} from '@/app/containers/SinkManage/api'
import {SEARCH_JAR_INFOS_API} from '@/app/containers/ResourceManage/api'
import DataSourceManageViewLogModal
  from "@/app/components/ResourceManage/DataSourceManage/DataSourceManageTopologyModal/DataSourceManageViewLogModal";

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({})
)
export default class SinkerTopologyWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      modalKey: 'modalKey',
      visible: false,
      modalStatus: 'create',
      sinkerTopologyInfo: null,
      jarList: [],

      sinkerTopologyList: [],

      startTopoModalVisible: false,
      startTopoModalKey: 'startTopoModalKey',
      startTopoModalLog: null,
      startTopoModalLoading: true,

      rerunModalVisible: false,
      rerunModalKey: 'rerunModalKey',
      rerunModalRecord: {},
      rerunInitResult: null,

      addSchemaModalVisible: false,
      addSchemaModalKey: 'addSchemaModalKey',
      addSchemaModalRecord: {},
      sinkerSchemaList: [],

      selectedRows: [],

      logModalKey: 'logModalKey',
      logModalVisible: false,
      logModalRecord: {},
      logModalContent: {},
      logModalLoading: null
    }
    this.tableWidth = [
      '5%',
      '10%',
      '10%',
      '50%',
      '10%',
      '200px'
    ]
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
  }

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns String 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  componentWillMount() {
    // 初始化查询
    this.handleSearch(this.initParams)
  }

  /**
   * @param params 查询的参数 type:[Object Object]
   * @description 查询Sinker列表
   */
  handleSearch = (params) => {
    Request(SEARCH_SINKER_TOPOLOGY_API, {
      params: params,
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            sinkerTopologyList: res.payload
          })
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

  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description sinkerTopology分页
   */
  handlePagination = page => {
    this.initParams = {...this.initParams, pageNum: page}
    this.handleSearch({...this.initParams})
  }

  handleOpenCreateModal = () => {
    this.handleLoadJarList()
    this.setState({visible: true, modalStatus: 'create', modalKey: this.handleRandom('sinker')})
  }

  handleCloseModal = () => {
    this.setState({visible: false, modalKey: this.handleRandom('sinker')})
  }

  handleOpenModifyModal = (record) => {
    this.handleGetSinkerTopologyInfo(record.id)
    this.handleLoadJarList()
    this.setState({visible: true, modalStatus: 'modify', modalKey: this.handleRandom('sinker')})
  }

  handleLoadJarList = () => {
    Request(SEARCH_JAR_INFOS_API, {
      params: {category: 'sinker', type: 'sinker'},
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            jarList: res.payload
          })
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

  handleGetSinkerTopologyInfo = (id) => {
    Request(`${SEARCH_SINKER_TOPOLOGY_BY_ID_API}/${id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            sinkerTopologyInfo: res.payload
          })
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

  handleStartOrStopTopo = (operate, record) => {
    this.setState({
      startTopoModalVisible: true,
      startTopoModalKey: this.handleRandom('startTopoModalKey'),
      startTopoModalLog: `发送${operate}命令中...`,
      startTopoModalLoading: true
    })

    Request(START_OR_STOP_TOPOLOGY_API, {
      data: {
        cmdType: operate, ...record
      },
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            startTopoModalLog: `${res.payload}`
          })
        } else {
          message.warn(res.message)
        }
        this.setState({
          startTopoModalLoading: false
        })
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({
          startTopoModalLoading: false
        })
      })
  }

  handleCloseTopoModal = () => {
    this.setState({startTopoModalVisible: false, startTopoModalLog: '', startTopoModalLoading: false})
    this.setState({startTopoModalLog: ''})
    this.handleSearch(this.initParams)
  }

  handleCloseRerunModal = () => {
    this.setState({
      rerunModalVisible: false
    })
  }

  handleOpenAddSchemaModal = (record) => {
    this.handleSearchSchemaList(record)
    this.setState({
      addSchemaModalVisible: true,
      addSchemaModalRecord: record,
      addSchemaModalKey: this.handleRandom('addSchemaModalKey')
    })
  }

  handleCloseAddSchemaModal = () => {
    this.setState({
      addSchemaModalVisible: false,
      sinkerSchemaList: []
    })
  }

  handleAddAllTableFlagChangeModal = (value, schemaId) => {
    const {sinkerSchemaList} = this.state
    sinkerSchemaList.map(item => {
      if (item.schemaId === schemaId) {
        item.addAllTable = value
      }
    })
  }

  handleSearchSchemaList = (record) => {
    Request(SEARCH_ALL_SINKER_SCHEMA_API, {
      params: {
        dsName: record.dsName,
        schemaName: record.schemaName,
        sinkerTopoId: record.id
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            sinkerSchemaList: res.payload
          })
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

  handleOpenRerunModal = (record) => {
    Request(GET_SINKER_TOPIC_INFOS_API, {
      params: {
        sinkerName: record.sinkerName
      },
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            rerunModalVisible: true,
            rerunModalRecord: record,
            rerunInitResult: res.payload
          })
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

  handleSetSelectRows = (selectedRows) => {
    this.setState({selectedRows: selectedRows})
  }

  handleOpenLogModal = (record) => {
    this.setState({
      logModalVisible: true,
      logModalRecord: record,
      logModalKey: this.handleRandom('logModalKey')
    })
    this.handleReadLog(record)
  }

  handleCloseLogModal = () => {
    this.setState({
      logModalVisible: false,
      logModalKey: this.handleRandom('logModalKey')
    })
  }

  handleReadLog = record => {
    this.setState({logModalLoading: true})
    Request(VIEW_LOG_API, {
      params: {sinkerName: record.sinkerName}
    })
      .then(res => {
        this.setState({logModalLoading: false})
        if (res && res.status === 0) {
          this.setState({logModalContent: res.payload})
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        this.setState({logModalLoading: false})
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  render() {
    const {modalKey, visible, modalStatus, sinkerTopologyInfo, sinkerTopologyList, jarList} = this.state
    const {startTopoModalVisible, startTopoModalLog, startTopoModalLoading, startTopoModalKey} = this.state
    const {rerunModalVisible, rerunModalKey, rerunModalRecord, rerunInitResult} = this.state
    const {addSchemaModalVisible, addSchemaModalRecord, addSchemaModalKey, sinkerSchemaList} = this.state
    const {selectedRows} = this.state
    const {logModalKey, logModalRecord, logModalVisible, logModalContent, logModalLoading} = this.state

    const {locale} = this.props
    const localeMessage = intlMessage(locale)
    const breadSource = [
      {
        path: '/sink-manage',
        name: 'home'
      },
      {
        path: '/sink-manage',
        name: 'Sink管理'
      },
      {
        path: '/sink-manage/sinker-manage',
        name: 'Sinker管理'
      }
    ]
    return (
      <div>
        <Helmet
          title="Sink"
          meta={[{name: 'description', content: 'Sink Manage'}]}
        />
        <Bread source={breadSource}/>
        <SinkerTopologyManageSearch
          locale={locale}
          sinkerParams={this.initParams}
          onOpen={this.handleOpenCreateModal}
          onSearch={this.handleSearch}
        />
        <SinkerTopologyManageGrid
          locale={locale}
          tableWidth={this.tableWidth}
          sinkerParams={this.initParams}
          sinkerList={sinkerTopologyList}
          onOpenModifyModal={this.handleOpenModifyModal}
          onSearch={this.handleSearch}
          onPagination={this.handlePagination}
          onMount={this.handleMount}
          onStartOrStopTopo={this.handleStartOrStopTopo}
          onOpenRerunModal={this.handleOpenRerunModal}
          onOpenAddSchemaModal={this.handleOpenAddSchemaModal}
          onOpenLogModal={this.handleOpenLogModal}
        />
        <SinkerTopologyForm
          modalKey={modalKey}
          locale={locale}
          modalStatus={modalStatus}
          visible={visible}
          sinkerInfo={sinkerTopologyInfo}
          jarList={jarList}
          sinkerParams={this.initParams}
          onSearch={this.handleSearch}
          onClose={this.handleCloseModal}
        />
        <ProjectTopologyStartModal
          visible={startTopoModalVisible}
          key={startTopoModalKey}
          onClose={this.handleCloseTopoModal}
          startTopoModalLog={startTopoModalLog}
          loading={startTopoModalLoading}
        />
        <SinkerTopologyRerunModal
          visible={rerunModalVisible}
          key={rerunModalKey}
          record={rerunModalRecord}
          onClose={this.handleCloseRerunModal}
          rerunInitResult={rerunInitResult}
          topologyRerunApi={DRAG_BACK_RUN_AGAIN_API}
        />
        <SinkerTopologyAddSchemaModal
          visible={addSchemaModalVisible}
          key={addSchemaModalKey}
          record={addSchemaModalRecord}
          onClose={this.handleCloseAddSchemaModal}
          sinkerSchemaList={sinkerSchemaList}
          onSearchSchemaList={this.handleSearchSchemaList}
          selectedRows={selectedRows}
          onSetSelectRows={this.handleSetSelectRows}
          onAddAllTableChange={this.handleAddAllTableFlagChangeModal}
        />
        <DataSourceManageViewLogModal
          key={logModalKey}
          visible={logModalVisible}
          record={logModalRecord}
          content={logModalContent}
          loading={logModalLoading}
          onClose={this.handleCloseLogModal}
          onRefresh={this.handleReadLog}
        />
      </div>
    )
  }
}
SinkerTopologyWrapper.propTypes = {}
