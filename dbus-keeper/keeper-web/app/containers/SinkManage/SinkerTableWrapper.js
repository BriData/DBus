import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {Bread, SinkerTableGrid, SinkerTableSearch} from '@/app/components/index'
// selectors
import {sinkerTableModel} from './selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
import {searchSinkerTableList, setSearchSinkerTableParam} from './redux'
import SinkerTableForm from "@/app/components/SinkManage/SinkerTable/SinkerTableForm";
import {message} from "antd";
import Request from "@/app/utils/request";
import {BATCH_DELETE_SINKER_TABLE_API} from "@/app/containers/SinkManage/api";
// action

// 链接reducer和action
@connect(
  createStructuredSelector({
    sinkerTableData: sinkerTableModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchSinkerTableList: param => dispatch(searchSinkerTableList.request(param)),
    setSearchSinkerTableParam: param => dispatch(setSearchSinkerTableParam(param))
  })
)
export default class SinkerTableWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      modifyTableKey: 'modifyTableKey',
      modifyTableVisible: false,
      modifyTableRecord: null,

      selectedRows: [],
      selectedRowKeys: []
    }
    this.tableWidth = [
      '6%',
      '10%',
      '10%',
      '10%',
      '15%',
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

  handleSearch = (params) => {
    const {searchSinkerTableList, setSearchSinkerTableParam} = this.props
    searchSinkerTableList(params)
    setSearchSinkerTableParam(params)
  }

  handleRefresh = () => {
    const {sinkerTableData} = this.props
    const {sinkerTableParams} = sinkerTableData
    this.handleSearch(sinkerTableParams)
  }

  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description sinkerTopology分页
   */
  handlePagination = page => {
    const {sinkerTableData} = this.props
    const {sinkerTableParams} = sinkerTableData
    // 分页查询并存储参数
    this.handleSearch({...sinkerTableParams, pageNum: page})
  }

  handleShowSizeChange = (current, size) => {
    const {sinkerTableData} = this.props
    const {sinkerTableParams} = sinkerTableData
    // 分页查询并存储参数
    this.handleSearch({...sinkerTableParams, pageNum: current, pageSize: size})
  }

  handleOpenModifyTableModal = (record) => {
    this.setState({
      modifyTableKey: this.handleRandom('modifyTableKey'),
      modifyTableVisible: true,
      modifyTableRecord: record
    })
  }

  handleCloseModifyTableModal = () => {
    this.setState({
      modifyTableVisible: false,
      modifyTableRecord: null
    })
  }

  handleSelectionChange = (selectedRowKeys, selectedRows) => {
    this.setState({selectedRowKeys, selectedRows})
  }

  handleBatchDeleteTable = () => {
    const {selectedRowKeys, selectedRows} = this.state
    if (!selectedRowKeys.length) {
      message.error('没有选中任何表')
      return
    }
    var ids = []
    selectedRows.map(item => ids.push(item.id))
    Request(`${BATCH_DELETE_SINKER_TABLE_API}`, {
      data: ids,
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

  render() {
    const {locale, sinkerTableData} = this.props
    const {modifyTableKey, modifyTableVisible, modifyTableRecord} = this.state
    const {
      sinkerTableList,
      sinkerTableParams
    } = sinkerTableData

    const {selectedRowKeys} = this.state

    const breadSource = [
      {
        path: '/sink-manage',
        name: 'home'
      },
      {
        path: '/sink-manage',
        name: 'sink管理'
      },
      {
        path: '/sink-manage/schema-manage',
        name: '表管理'
      }
    ]
    return (
      <div>
        <Helmet
          title="Sink"
          meta={[{name: 'description', content: 'Sink Manage'}]}
        />
        <Bread source={breadSource}/>
        <SinkerTableSearch
          locale={locale}
          searchParams={sinkerTableParams}
          onSearch={this.handleSearch}
          onBatchDeleteTable={this.handleBatchDeleteTable}
        />
        <SinkerTableGrid
          locale={locale}
          tableWidth={this.tableWidth}
          searchParams={sinkerTableParams}
          sinkerTableList={sinkerTableList.result.payload}
          onModify={this.handleOpenModifyTableModal}
          onSearch={this.handleSearch}
          onPagination={this.handlePagination}
          onMount={this.handleMount}
          onSelectionChange={this.handleSelectionChange}
          selectedRowKeys={selectedRowKeys}
          onShowSizeChange={this.handleShowSizeChange}
        />
        <SinkerTableForm
          visible={modifyTableVisible}
          key={modifyTableKey}
          record={modifyTableRecord}
          onClose={this.handleCloseModifyTableModal}
          onSearch={this.handleSearch}
          searchParams={sinkerTableParams}
        />
      </div>
    )
  }
}
SinkerTableWrapper.propTypes = {
  locale: PropTypes.any,
  searchSinkerTableList: PropTypes.func,
  setSearchSinkerTableParam: PropTypes.func
}
