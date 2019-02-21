import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
// 导入自定义组件
import {
  BathchRestatrTopoForm,
  BathchRestatrTopoSearch,
  BathchRestatrTopoRestart
} from '@/app/components'

import {DataSourceModel, JarManageModel} from './selectors'
// action
import {
  setDataSourceParams,
  searchDatasourceList,
  searchJarInfos
} from './redux'

import {BATCH_RESTART_TOPO_API} from "./api";

// 链接reducer和action
@connect(
  createStructuredSelector({
    JarManageData: JarManageModel(),
    dataSourceData: DataSourceModel()
  }),
  dispatch => ({
    setDataSourceParams: param => dispatch(setDataSourceParams(param)),
    searchDatasourceList: param => dispatch(searchDatasourceList.request(param)),
    searchJarInfos: param => dispatch(searchJarInfos.request(param))
  })
)
export default class BatchRestartTopoWrapper extends Component {
  constructor(props) {
    super(props)
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
    this.state = {
      topoModalKey: 'topoModalKey',
      topoModalVisible: false,

      selectedRowKeys: [],
      selectedRows: []
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
    const {searchDatasourceList, setDataSourceParams} = this.props
    searchDatasourceList(params)
    if (boolean || boolean === undefined) {
      setDataSourceParams(params)
    }
    this.setState({
      selectedRowKeys: [],
      selectedRows: []
    })
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


  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`


  handleSelectionChange = (selectedRowKeys, selectedRows) => {
    this.setState({selectedRowKeys, selectedRows})
  }

  handleOpenTopoModal = () => {
    const {selectedRows} = this.state
    if (selectedRows.length === 0) {
      message.warn("未选中任数据源")
      return
    }
    const {locale, dataSourceData} = this.props
    const {dataSourceParams} = dataSourceData
    if (dataSourceParams.dsType === undefined || dataSourceParams.dsType === null) {
      message.warn("请选择数据源类型")
      return
    }
    this.setState({
      topoModalVisible: true
    })
  }

  handleRefresh = () => {
    const {dataSourceData} = this.props
    const {dataSourceParams} = dataSourceData
    this.handleSearch({...dataSourceParams})
  }

  handleCloseTopoModal = record => {
    this.setState({
      topoModalKey: this.handleRandom('topo'),
      topoModalVisible: false,
      selectedRowKeys: [],
      selectedRows: []
    })
  }

  render() {
    console.info(this.props)
    const {locale, dataSourceData} = this.props
    const {dataSourceParams, dataSourceList} = dataSourceData
    const jarInfos = this.props.JarManageData.jarInfos.result.payload || []
    const {searchJarInfos} = this.props
    const {selectedRowKeys, selectedRows, topoModalVisible, topoModalKey} = this.state
    return (
      <div>
        <BathchRestatrTopoSearch
          params={dataSourceParams}
          onSearch={this.handleSearch}
          onOpenRestart={this.handleOpenTopoModal}
        />
        <BathchRestatrTopoForm
          dataSourceList={dataSourceList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onDBusData={this.handleDBusData}
          onRefresh={this.handleRefresh}
          onSelectionChange={this.handleSelectionChange}
          selectedRowKeys={selectedRowKeys}
        />
        <BathchRestatrTopoRestart
          key={topoModalKey}
          visible={topoModalVisible}
          selectedRows={selectedRows}
          onClose={this.handleCloseTopoModal}
          jarInfos={jarInfos}
          onSearchJarInfos={searchJarInfos}
          params={dataSourceParams}
          batchRestartTopoApi={BATCH_RESTART_TOPO_API}
        />
      </div>
    )
  }
}
BatchRestartTopoWrapper.propTypes = {
  locale: PropTypes.any
}
