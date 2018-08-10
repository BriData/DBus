import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
// 导入自定义组件
import { Bread, GlobalFullpullForm } from '@/app/components'
import { makeSelectLocale } from '../LanguageProvider/selectors'
import {ControlMessageModel,GlobalFullpullModel} from './selectors'
import {DataSchemaModel, DataTableModel, JarManageModel} from '../ResourceManage/selectors'
import {
  searchDataSourceList,
  globalFullPull
} from '@/app/containers/toolSet/redux'
import {
  searchAllDataSchema,
  findAllDataTableList,
  searchJarInfos
} from '../ResourceManage/redux'
import {
  TOPO_JAR_START_API
} from '../ResourceManage/api'
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    ControlMessageData: ControlMessageModel(),
    GlobalFullpullData: GlobalFullpullModel(),
    DataSchemaData: DataSchemaModel(),
    dataTableData: DataTableModel(),
    JarManageData: JarManageModel(),
  }),
  dispatch => ({
    searchDataSourceList: param => dispatch(searchDataSourceList.request(param)),
    searchAllDataSchema: param => dispatch(searchAllDataSchema.request(param)),
    findAllDataTableList: param => dispatch(findAllDataTableList.request(param)),
    globalFullPull: param => dispatch(globalFullPull.request(param)),
    searchJarInfos: param => dispatch(searchJarInfos.request(param)),
  })
)
export default class GlobalFullpullWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }
  componentWillMount = () => {
    const {searchDataSourceList} = this.props
    searchDataSourceList()
    const {searchJarInfos} = this.props
    searchJarInfos({
      category: 'normal',
      version: '',
      type: ''
    })
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleSearchSchema = dsId => {
    const {searchAllDataSchema} = this.props
    searchAllDataSchema({dsId})
  }

  handleSearchTable = (dsId, schemaName)=> {
    const {findAllDataTableList} = this.props
    findAllDataTableList({dsId, schemaName})
  }

  handleGlobalFullPull = data => {
    const {globalFullPull} = this.props
    globalFullPull(data)
  }

  render () {
    console.info(this.props)
    const dataSourceList = this.props.ControlMessageData.dataSourceList.result.payload || []
    const dataSchemaList = Object.values(this.props.DataSchemaData.allDataSchemaList.result)
    const dataTableList = Object.values(this.props.dataTableData.allDataTableList.result)

    const jarInfos = this.props.JarManageData.jarInfos.result.payload || []
    return (
      <div>
        <GlobalFullpullForm
          dataSourceList={dataSourceList}
          dataSchemaList={dataSchemaList}
          dataTableList={dataTableList}
          onSearchSchema={this.handleSearchSchema}
          onSearchTable={this.handleSearchTable}
          onGlobalFullPull={this.handleGlobalFullPull}
          jarInfos={jarInfos}
          topoJarStartApi={TOPO_JAR_START_API}
        />
      </div>
    )
  }
}
GlobalFullpullWrapper.propTypes = {
  locale: PropTypes.any,
}
