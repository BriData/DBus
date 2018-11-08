import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {ProjectResourceViewEncodeModal, ProjectResourceGrid, ProjectResourceSearch, Bread } from '@/app/components'
// 修改弹框
import { ProjectHome } from '@/app/components'
// selectors
import { ProjectResourceModel, ProjectTableModel } from './selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  getProjectInfo,
  searchAllResourceProject,
  searchAllResourceDsName,
  searchAllResource,
  searchAllResourceTableEncode,
  setAllResourceParams
} from './redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectResourceData: ProjectResourceModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchAllResourceProject: param => dispatch(searchAllResourceProject.request(param)),
    searchAllResourceDsName: param => dispatch(searchAllResourceDsName.request(param)),
    getResourceList: param => dispatch(searchAllResource.request(param)),
    setResourceParams: param => dispatch(setAllResourceParams(param)),
    getProjectInfo: param => dispatch(getProjectInfo.request(param)),
    searchAllResourceTableEncode: param => dispatch(searchAllResourceTableEncode.request(param))
  })
)
export default class ProjectResourceWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      modalVisibal: false,
      modalStatus: 'modify',
      modalKey: '00000000000000000000000',

      encodeModalVisible: false,
      encodeModalRecord: {}
    }
    this.tableWidth = [
      '10%',
      '8%',
      '8%',
      '8%',
      '8%',
      '10%',
      '10%',
      '6%',
      '6%',
      '17%',
      '5%',
      '100px',
      '120px'
    ]
    this.initParams = {
      pageNum: 1,
      pageSize: 10,
      isUser: false // 用于判断是否是User身份，请求的API不同
    }
    this.projectId = null
  }
  componentWillMount () {
    const {searchAllResourceProject, searchAllResourceDsName, projectId} = this.props
    // 获取project 如果没有projectId，身份为Admin，则取所有的project列表
    !projectId && searchAllResourceProject()

    this.projectId = projectId
    // 获取 DataSource
    searchAllResourceDsName({projectId: projectId})
    // 初始化查询，有projectId，身份为User
    projectId && (this.initParams.projectId = projectId, this.initParams.isUser = true)
    this.handleSearch(this.initParams, true)
  }
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params, boolean) => {
    const { getResourceList, setResourceParams } = this.props
    if (boolean) {
      // 存储
      setResourceParams(params)
    }
    // 查询
    getResourceList(params)
  };

  handleUserViewEncode = (record) => {
    const {searchAllResourceTableEncode} = this.props
    searchAllResourceTableEncode({projectId: record.projectId, tableId: record.tableId})
    this.setState({encodeModalVisible: true, encodeModalRecord: record})
  }

  handleModify = (id) => {
    const { getProjectInfo } = this.props
    getProjectInfo({ id })
    this.stateModalVisibal(true, 'modify')
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
   * 新建和编辑弹窗是否显示
   */
  stateModalVisibal = (modalVisibal, modalStatus = 'modify') => {
    this.setState({
      modalVisibal,
      modalStatus,
      modalKey: this.handleRandom('modal')
    })
  }

  handleSearchProject = () => {
    const { projectResourceData } = this.props
    const { resourceParams } = projectResourceData
    this.handleSearch(resourceParams)
  };

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const { projectResourceData } = this.props
    const { resourceParams } = projectResourceData
    // 分页查询并存储参数
    this.handleSearch({ ...resourceParams, pageNum: page }, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const { projectResourceData } = this.props
    const { resourceParams } = projectResourceData
    this.handleSearch(
      { ...resourceParams, pageNum: current, pageSize: size },
      true
    )
  }

  render () {
    const {
      modalVisibal,
      modalStatus,
      modalKey
    } = this.state
    const {
      encodeModalVisible,
      encodeModalRecord
    } = this.state
    const { locale, projectResourceData, setResourceParams } = this.props
    const {
      resourceList,
      dsNameList,
      projectList,
      resourceParams,
      encodeList
    } = projectResourceData

    const breadSource = [
      {
        path: '/project-manage',
        name: 'home'
      },
      {
        path: '/project-manage',
        name: '项目管理'
      },
      {
        name: 'Resource'
      }
    ]
    this.props.projectDisplayName && breadSource.push({
      name: this.props.projectDisplayName
    })
    return (
      <div>
        <Helmet
          title="ProjectResource"
          meta={[
            { name: 'description', content: 'Description of ProjectResource' }
          ]}
        />
        <Bread source={breadSource} />
        <ProjectResourceSearch
          locale={locale}
          projectId={this.projectId}
          resourceParams={resourceParams}
          projectList={projectList && projectList.result && projectList.result.payload}
          dataSourceList={dsNameList && dsNameList.result && dsNameList.result.payload}
          onSetResourceParams={setResourceParams}
          onSearch={this.handleSearch}
        />
        <ProjectResourceGrid
          locale={locale}
          projectId={this.projectId}
          tableWidth={this.tableWidth}
          resourceList={resourceList}
          onModify={this.handleModify}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onViewEncode={this.handleUserViewEncode}
        />
        <ProjectHome
          modalKey={modalKey}
          modalVisibal={modalVisibal}
          modalStatus={modalStatus}
          initTab={'resource'}
          disabled
          onCloseModal={this.stateModalVisibal}
          onSearchProject={this.handleSearchProject}
        />
        <ProjectResourceViewEncodeModal
          visible={encodeModalVisible}
          record={encodeModalRecord}
          encodeList={encodeList}
          onClose={() => this.setState({encodeModalVisible: false})}
        />
      </div>
    )
  }
}
ProjectResourceWrapper.propTypes = {
  locale: PropTypes.any,
  projectResourceData: PropTypes.object,
  getResourceList: PropTypes.func,
  setResourceParams: PropTypes.func
}
