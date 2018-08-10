import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import { message } from 'antd'
// 导入自定义组件
import {
  Bread,
  EncodeManagerSearch,
  EncodeManagerGrid,
} from '@/app/components/index'
// selectors
import { ProjectTableModel } from '@/app/containers/ProjectManage/selectors/index'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  searchEncodeList,
  setEncodeManagerParams
} from '../ResourceManage/redux/index'
import {
  EncodeManagerModel
} from '../ResourceManage/selectors/index'
import {getProjectList, getTopologyList} from "@/app/containers/ProjectManage/redux/index"
import {searchDataSourceIdTypeName} from "@/app/containers/ResourceManage/redux/index"
import {DataSourceModel} from "@/app/containers/ResourceManage/selectors";

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectTableData: ProjectTableModel(),
    EncodeManagerData: EncodeManagerModel(),
    dataSourceData: DataSourceModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    getProjectList: param => dispatch(getProjectList.request(param)),
    getTopologyList: param => dispatch(getTopologyList.request(param)),
    searchDataSourceIdTypeName: param => dispatch(searchDataSourceIdTypeName.request(param)),

    searchEncodeList: param => dispatch(searchEncodeList.request(param)),
    setEncodeManagerParams: param => dispatch(setEncodeManagerParams(param)),
  })
)
export default class EncodeManageWrapper extends Component {
  constructor (props) {
    super(props)
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
  }
  componentWillMount () {
    const {
      getProjectList,
      searchDataSourceIdTypeName,
      getTopologyList,
    } = this.props
    this.handleSearch({...this.initParams}, true)
    // 获取project
    getProjectList()
    // 获取 DataSource
    searchDataSourceIdTypeName()
    // 获取TopoList
    getTopologyList()
  }

  handleProjectSelect = value => {
    const {
      getTopologyList,
    } = this.props
    getTopologyList({projectId: value})
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
    const { searchEncodeList, setEncodeManagerParams } = this.props
    // 查询
    searchEncodeList(params)
    if (boolean) {
      // 存储
      setEncodeManagerParams(params)
    }
  };
  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const { EncodeManagerData } = this.props
    const { params } = EncodeManagerData
    // 分页查询并存储参数
    this.handleSearch({ ...params, pageNum: page }, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const { EncodeManagerData } = this.props
    const { params } = EncodeManagerData
    this.handleSearch(
      { ...params, pageNum: current, pageSize: size },
      true
    )
  };

  render () {
    console.info(this.props)
    const {
      projectTableData,
      EncodeManagerData
    } = this.props
    const {
      topologyList,
      projectList,
    } = projectTableData
    const {dataSourceData} = this.props
    const {dataSourceIdTypeName} = dataSourceData

    const {
      params,
      encodeList
    } = EncodeManagerData
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
        path: '/resource-manage/encode-manager',
        name: '脱敏配置查看'
      }
    ]
    return (
      <div>
        <Helmet
          title="Encode Manage"
          meta={[
            { name: 'description', content: 'Description of Encode Manage' }
          ]}
        />
        <Bread source={breadSource} />
        <EncodeManagerSearch
          dataSourceList={dataSourceIdTypeName}
          topologyList={topologyList}
          projectList={projectList}
          onSearch={this.handleSearch}
          onProjectSelect={this.handleProjectSelect}
          params={params}
        />
        <EncodeManagerGrid
          encodeList={encodeList}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
        />
      </div>
    )
  }
}
EncodeManageWrapper.propTypes = {
  locale: PropTypes.any,
  projectTableData: PropTypes.object,
  getProjectList: PropTypes.func,
  getTopologyList: PropTypes.func,
}
