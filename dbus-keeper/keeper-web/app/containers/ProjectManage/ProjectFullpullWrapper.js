import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import {ProjectFullpullGrid, ProjectFullpullSearch, ProjectFullpullModifyModal, Bread} from '@/app/components'
// 修改弹框
// selectors
import {ProjectFullpullModel} from './selectors'
import {makeSelectLocale} from '../LanguageProvider/selectors'
// action
import {
  searchAllFullpull,
  searchAllFullpullProject,
  searchAllFullpullDsName,
  setAllFullpullParams
} from './redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectFullpullData: ProjectFullpullModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    searchAllFullpullProject: param => dispatch(searchAllFullpullProject.request(param)),
    searchAllFullpullDsName: param => dispatch(searchAllFullpullDsName.request(param)),
    getFullpullList: param => dispatch(searchAllFullpull.request(param)),
    setFullpullParams: param => dispatch(setAllFullpullParams(param)),
  })
)
export default class ProjectFullpullWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      modalVisible: false,
      modalRecord: {},
      modalKey: 'modalKey',
      query: {},

    }
    this.tableWidth = [
      '8%',
      '8%',
      '7%',
      '10%',
      '8%',
      '12%',
      '10%',
      '10%',
      '8%',
      '10%',
      '8%',
      '8%',
      '8%',
      '10%'
    ]
    this.initParams = {
      pageNum: 1,
      pageSize: 10,
      isUser: false // 用于判断是否是User身份，请求的API不同
    }
  }

  componentWillMount() {
    let query = {}
    // 用户身份，需要从属性中获取query
    if (this.props.query) {
      query = {...this.props.query}
      this.initParams.isUser = true
    }
    // 管理员身份，需要从地址栏中获取query
    if (this.props.location && this.props.location.query) {
      query = {...this.props.location.query}
    }
    this.setState({query})

    const {searchAllFullpullProject, searchAllFullpullDsName} = this.props
    searchAllFullpullProject()
    searchAllFullpullDsName()

    this.initParams = {
      ...this.initParams,
      projectName: query.projectName,
      dsName: query.dsName,
      schemaName: query.schemaName,
      tableName: query.tableName,
    }
    delete this.initParams.projectDisplayName
    this.handleSearch(this.initParams, true)
  }

  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params, boolean) => {
    const {getFullpullList, setFullpullParams} = this.props

    if (boolean) {
      // 存储
      setFullpullParams(params)
    }
    // 查询
    if (params.projectName === 'null') delete params.projectName
    if (params.dsName === 'null') delete params.dsName
    if (params.schemaName === 'null') delete params.schemaName
    if (params.tableName === 'null') delete params.tableName
    if (params.id === 'null') delete params.id
    if (params.orderBy === 'null') delete params.orderBy
    if (params.targetSinkTopic === 'null') delete params.targetSinkTopic
  
    getFullpullList(params)
  };

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const {projectFullpullData} = this.props
    const {fullpullParams} = projectFullpullData
    // 分页查询并存储参数
    this.handleSearch({...fullpullParams, pageNum: page}, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const {projectFullpullData} = this.props
    const {fullpullParams} = projectFullpullData
    this.handleSearch(
      {...fullpullParams, pageNum: current, pageSize: size},
      true
    )
  }

  handleOpenModifyModal = (record) => {
    this.setState({
      modalKey: this.handleRandom("modalKey"),
      modalRecord: record,
      modalVisible: true
    })
  }

  handleCloseModifyModal = () => {
    const {projectFullpullData} = this.props
    const {fullpullParams} = projectFullpullData
    this.handleSearch(fullpullParams)
    this.setState({
      modalVisible: false
    })
  }

  render() {
    const {locale, projectFullpullData, setFullpullParams} = this.props
    const {
      fullpullList,
      dsNameList,
      projectList,
      fullpullParams
    } = projectFullpullData
    const {modalVisible, modalKey, modalRecord} = this.state
    const breadSource = [
      {
        path: '/project-manager',
        name: 'home'
      },
      {
        path: '/project-manage',
        name: '项目管理'
      },
      {
        name: '全量拉取历史'
      }
    ]
    this.state.query.projectDisplayName && breadSource.push({
      name: this.state.query.projectDisplayName
    })
    return (
      <div>
        <Helmet
          title="Fullpull History"
          meta={[
            {name: 'description', content: 'Description of ProjectFullpull'}
          ]}
        />
        <Bread source={breadSource}/>
        <ProjectFullpullSearch
          query={this.state.query}
          locale={locale}
          fullpullParams={fullpullParams}
          projectList={projectList && projectList.result && projectList.result.payload}
          dataSourceList={dsNameList && dsNameList.result && dsNameList.result.payload}
          onSetFullpullParams={setFullpullParams}
          onSearch={this.handleSearch}
        />
        <ProjectFullpullGrid
          locale={locale}
          tableWidth={this.tableWidth}
          fullpullList={fullpullList}
          onModify={this.handleOpenModifyModal}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
        />
        <ProjectFullpullModifyModal
          key={modalKey}
          visible={modalVisible}
          fullpullInfo={modalRecord}
          onClose={this.handleCloseModifyModal}
        />
      </div>
    )
  }
}
ProjectFullpullWrapper.propTypes = {
  locale: PropTypes.any,
  projectFullpullData: PropTypes.object,
  getFullpullList: PropTypes.func,
  setFullpullParams: PropTypes.func
}
