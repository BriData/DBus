import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {
  Bread,
  DataSourceCreateTabs
} from '@/app/components'
// selectors
import {DataSourceModel, DataSourceCreateModel} from './selectors'
import {ZKManageModel} from "@/app/containers/ConfigManage/selectors"
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {
  getSchemaListByDsId,
  getSchemaTableList,
  getLatestJarPath
} from './redux'
import {
  loadZkTreeByDsName,
  readZkData,
  saveZkData
} from "@/app/containers/ConfigManage/redux"
import {
  DATA_SOURCE_ADD_API,
  DATA_SOURCE_VALIDATE_API,
  DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API,
  CLONE_CONF_FROM_TEMPLATE_API,
  TOPO_JAR_START_API
} from './api'


// 链接reducer和action
@connect(
  createStructuredSelector({
    DataSourceCreateData: DataSourceCreateModel(),
    dataSourceData: DataSourceModel(),
    ZKManageData: ZKManageModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    getSchemaListByDsId: param => dispatch(getSchemaListByDsId.request(param)),
    getSchemaTableList: param => dispatch(getSchemaTableList.request(param)),
    getLatestJarPath: param => dispatch(getLatestJarPath.request(param)),

    readZkData: param => dispatch(readZkData.request(param)),
    saveZkData: param => dispatch(saveZkData.request(param)),
    loadZkTreeByDsName: param => dispatch(loadZkTreeByDsName.request(param)),
  })
)
export default class DataSourceCreateWrapper extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {

  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  render() {
    console.info(this.props)
    const {getSchemaListByDsId, getSchemaTableList} = this.props
    const {dataSourceData} = this.props
    const schemaList = dataSourceData.schemaList.result.payload || []
    const schemaTableResult = dataSourceData.schemaTableResult

    const {loadZkTreeByDsName,readZkData,saveZkData} = this.props
    const zkData = (this.props.ZKManageData.zkData.result.payload || {}).content
    const tree = this.props.ZKManageData.dataSourceZkTree.result.payload ?
      [this.props.ZKManageData.dataSourceZkTree.result.payload] : []

    const {getLatestJarPath} = this.props
    const jarPath = this.props.DataSourceCreateData.jarPath.result.payload || []
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
        path: '/resource-manage/datasource-create',
        name: '新建数据线'
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
        <DataSourceCreateTabs
          addDataSourceApi={DATA_SOURCE_ADD_API}
          validateDataSourceApi={DATA_SOURCE_VALIDATE_API}
          addSchemaTableApi={DATA_SOURCE_ADD_SCHEMA_TABLE_LIST_API}
          cloneConfFromTemplateApi={CLONE_CONF_FROM_TEMPLATE_API}
          topoJarStartApi={TOPO_JAR_START_API}

          getSchemaListByDsId={getSchemaListByDsId}
          getSchemaTableList={getSchemaTableList}
          loadZkTreeByDsName={loadZkTreeByDsName}
          readZkData={readZkData}
          saveZkData={saveZkData}
          getLatestJarPath={getLatestJarPath}

          schemaList={schemaList}
          schemaTableResult={schemaTableResult}
          tree={tree}
          zkData={zkData}
          jarPath={jarPath}
        />
      </div>
    )
  }
}
DataSourceCreateWrapper.propTypes = {
  locale: PropTypes.any,
}
