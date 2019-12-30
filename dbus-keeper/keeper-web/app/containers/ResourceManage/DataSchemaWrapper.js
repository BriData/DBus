import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {
  Bread,
  DataSchemaManageSearch,
  DataSchemaManageGrid,
  DataSchemaManageModifyModal,
  DataSchemaManageAddModal,
  DataSchemaManageAddLogModal,
  DataSchemaManageRerunModal,
  DataSchemaMoveModal
} from '@/app/components'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {DataSchemaModel, DataSourceModel} from './selectors'
import {
  searchDataSourceIdTypeName,
  setDataSchemaParams,
  searchDataSchemaList,
  getDataSchemaById,
  getSchemaTableList,
  cleanSchemaTable
} from './redux'

import {
  DATA_SCHEMA_UPDATE_API,
  DATA_SCHEMA_DELETE_API
} from './api'

import {
  DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API
} from './api'
import {message} from "antd";

// 链接reducer和action
@connect(
  createStructuredSelector({
    dataSourceData: DataSourceModel(),
    dataSchemaData: DataSchemaModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchDataSourceIdTypeName: param => dispatch(searchDataSourceIdTypeName.request(param)),

    setDataSchemaParams: param => dispatch(setDataSchemaParams(param)),
    searchDataSchemaList: param => dispatch(searchDataSchemaList.request(param)),
    getDataSchemaById: param => dispatch(getDataSchemaById.request(param)),
    getSchemaTableList: param => dispatch(getSchemaTableList.request(param)),
    cleanSchemaTable: param => dispatch(cleanSchemaTable(param)),
  })
)
export default class DataSchemaWrapper extends Component {
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

      addModalKey: 'addModalKey',
      addModalVisible: false,
      addModalRecord: {},

      addLogModalKey: 'addLogModalKey',
      addLogModalVisible: false,
      addLogModalRecord: {},

      rerunModalKey: 'rerunModalKey',
      rerunModalRecord: {},
      rerunModalVisible: false,

      moveSchemaModalVisible: false,
      moveSchemaRecord: {},
      moveSchemaModalKey: 'moveSchema'
    }
  }

  componentWillMount() {
    // 初始化查询
    const {searchDataSourceIdTypeName} = this.props
    searchDataSourceIdTypeName()
    this.handleSearch(this.initParams)
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleSearch = (params, boolean) => {
    const {searchDataSchemaList, setDataSchemaParams} = this.props
    searchDataSchemaList(params)
    if (boolean || boolean === undefined) {
      setDataSchemaParams(params)
    }
  }

  handlePagination = page => {
    const {dataSchemaData} = this.props
    const {dataSchemaParams} = dataSchemaData
    // 分页查询并存储参数
    this.handleSearch({...dataSchemaParams, pageNum: page})
  }

  handleShowSizeChange = (current, size) => {
    const {dataSchemaData} = this.props
    const {dataSchemaParams} = dataSchemaData
    // 分页查询并存储参数
    this.handleSearch({...dataSchemaParams, pageNum: current, pageSize: size})
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
    this.handleRefresh()
  }

  handleAdd = record => {
    if (record.ds_type === 'oracle' || record.ds_type === 'mysql' || record.ds_type === 'mongo'
      || record.ds_type === 'db2'
    ) {
      this.handleOpenAddModal(record)
    } else {
      this.handleOpenAddLogModal(record)
    }
  }

  handleOpenAddModal = record => {
    const {cleanSchemaTable, getSchemaTableList} = this.props
    cleanSchemaTable()
    this.setState({
      addModalVisible: true,
      addModalRecord: record
    }, () => getSchemaTableList({
      dsId: record.ds_id,
      dsName: record.ds_name,
      schemaName: record.schema_name
    }))
  }

  handleCloseAddModal = () => {
    this.setState({
      addModalKey: this.handleRandom('addModalKey'),
      addModalVisible: false,
    })
  }

  handleOpenAddLogModal = record => {
    this.setState({
      addLogModalVisible: true,
      addLogModalRecord: record
    })
  }

  handleCloseAddLogModal = () => {
    this.setState({
      addLogModalKey: this.handleRandom('addLogModalKey'),
      addLogModalVisible: false,
    })
  }

  handleRefresh = () => {
    const {dataSchemaData} = this.props
    const {dataSchemaParams} = dataSchemaData
    this.handleSearch({...dataSchemaParams})
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

  handleOpenMoveSchemaModal = record => {
    this.setState({
      moveSchemaModalVisible: true,
      moveSchemaRecord: record,
      moveSchemaModalKey: this.handleRandom('moveTables')
    })
  }

  handleCloseMoveSchemaModal = () => {
    this.setState({
      moveSchemaModalVisible: false
    })
  }

  render() {
    console.info(this.props)
    const {dataSourceData} = this.props
    const {dataSourceIdTypeName} = dataSourceData

    const {dataSchemaData} = this.props
    const {dataSchemaParams, dataSchemaList} = dataSchemaData

    const {modifyModalKey, modifyModalVisible, modifyModalRecord} = this.state

    const {addModalKey, addModalVisible, addModalRecord} = this.state
    const schemaTableResult = dataSourceData.schemaTableResult

    const {addLogModalKey, addLogModalVisible, addLogModalRecord} = this.state
    const {rerunModalKey, rerunModalRecord, rerunModalVisible} = this.state
    const {moveSchemaModalVisible, moveSchemaModalKey, moveSchemaRecord} = this.state
    const breadSchema = [
      {
        path: '/resource-manage',
        name: 'home'
      },
      {
        path: '/resource-manage',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/data-schema',
        name: 'DataSchema管理'
      }
    ]
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            {name: 'description', content: 'Description of DataSchema Manage'}
          ]}
        />
        <Bread source={breadSchema}/>
        <DataSchemaManageSearch
          dataSourceIdTypeName={dataSourceIdTypeName}
          params={dataSchemaParams}
          onSearch={this.handleSearch}
        />
        <DataSchemaManageGrid
          dataSchemaList={dataSchemaList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onModify={this.handleOpenModifyModal}
          onAdd={this.handleAdd}
          deleteApi={DATA_SCHEMA_DELETE_API}
          onRefresh={this.handleRefresh}
          onRerun={this.handleOpenRerunModal}
          onMoveSchema={this.handleOpenMoveSchemaModal}
        />
        <DataSchemaManageModifyModal
          key={modifyModalKey}
          visible={modifyModalVisible}
          schemaInfo={modifyModalRecord}
          onClose={this.handleCloseModify}
          updateApi={DATA_SCHEMA_UPDATE_API}
        />
        <DataSchemaManageAddModal
          key={addModalKey}
          visible={addModalVisible}
          record={addModalRecord}
          onClose={this.handleCloseAddModal}
          schemaTableResult={schemaTableResult}
          addApi={DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API}
        />
        <DataSchemaManageAddLogModal
          key={addLogModalKey}
          visible={addLogModalVisible}
          record={addLogModalRecord}
          onClose={this.handleCloseAddLogModal}
          addApi={DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API}
        />
        <DataSchemaManageRerunModal
          key={rerunModalKey}
          visible={rerunModalVisible}
          record={rerunModalRecord}
          onClose={this.handleCloseRerunModal}
        />
        <DataSchemaMoveModal
          key={moveSchemaModalKey}
          visible={moveSchemaModalVisible}
          onClose={this.handleCloseMoveSchemaModal}
          record={moveSchemaRecord}
          dataSourceIdTypeName={dataSourceIdTypeName}
          onRefresh={this.handleRefresh}
        />
      </div>
    )
  }
}
DataSchemaWrapper.propTypes = {
  locale: PropTypes.any,
}
