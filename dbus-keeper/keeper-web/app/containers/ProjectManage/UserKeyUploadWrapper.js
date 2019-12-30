import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
import Request, {setToken} from '@/app/utils/request'
// 导入自定义组件
import {
  Bread,
  UserKeyUploadSearch,
  UserKeyUploadGrid,
  UserKeyUploadModal
} from '@/app/components/index'
// selectors
import { makeSelectLocale } from '../LanguageProvider/selectors'
import {ProjectSummaryModel} from "@/app/containers/ProjectManage/selectors/index"
// action
import {searchProject} from "@/app/components/ProjectManage/ProjectSummary/redux/action/index";
import {
  UPLOAD_USER_KEY_API,
  SEARCH_USER_KEY_API
} from '../ResourceManage/api/index'



// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    ProjectSummaryData: ProjectSummaryModel(),
  }),
  dispatch => ({
    searchProject: param => dispatch(searchProject.request(param)),
  })
)
export default class UserKeyUploadWrapper extends Component {
  constructor (props) {
    super(props)

    this.state = {
      modalVisible: false,
      modalKey: 'modal',
      pageNum: 1,
      pageSize: 10,
      keyList: {}
    }
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillMount () {
    this.handleSearch()
  }

  handleSearch = (param) => {
    const {pageNum, pageSize} = this.state
    const combinedParam = {pageNum, pageSize, ...param}
    this.setState({
      ...combinedParam
    })
    Request(SEARCH_USER_KEY_API, {
      params: combinedParam,
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            keyList: res.payload
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

  handlePagination = page => {
    // 分页查询并存储参数
    this.handleSearch({pageNum: page})
  }

  handleShowSizeChange = (current, size) => {
    // 分页查询并存储参数
    this.handleSearch({pageNum: current, pageSize: size})
  }

  handleOpenUploadModal = () => {
    this.setState({
      modalVisible: true
    })
    const {searchProject} = this.props
    searchProject()
  }

  handleCloseUploadModal = () => {
    this.setState({
      modalKey: this.handleRandom('modal'),
      modalVisible: false
    })
    // this.handleSearch()
  }


  render () {
    const breadSource = [
      {
        path: '/project/home',
        name: 'home'
      },
      {
        path: '/resource-manage/data-source',
        name: '数据源管理'
      },
      {
        path: '/resource-manage/user-key-upload-manager',
        name: '用户密钥管理'
      }
    ]

    const {modalVisible, modalKey} = this.state
    const projectList = Object.values(this.props.ProjectSummaryData.projectList.result)

    const {keyList} = this.state
    console.info(this.props)
    return (
      <div>
        <Helmet
          title="数据源管理"
          meta={[
            { name: 'description', content: 'Description of DataSource Manage' }
          ]}
        />
        <Bread source={breadSource} />
        <UserKeyUploadSearch
          onOpenUploadModal={this.handleOpenUploadModal}
        />
        <UserKeyUploadGrid
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          keyList={keyList}
        />
        <UserKeyUploadModal
          visible={modalVisible}
          key={modalKey}
          onClose={this.handleCloseUploadModal}
          projectList={projectList}
          api={UPLOAD_USER_KEY_API}
        />
      </div>
    )
  }
}
UserKeyUploadWrapper.propTypes = {
  locale: PropTypes.any
}
