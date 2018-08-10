import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
import Request from '@/app/utils/request'
// 导入自定义组件
import {
  Bread,
  EncodePluginSearch,
  EncodePluginGrid,
  EncodePluginUploadModal
} from '@/app/components/index'
// selectors
import { EncodePluginManageModel } from '../ResourceManage/selectors/index'
import {ProjectSummaryModel} from "@/app/containers/ProjectManage/selectors/index"
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  setEncodePluginParam,
  searchEncodePlugin } from '../ResourceManage/redux/index'
import {searchProject} from "@/app/components/ProjectManage/ProjectSummary/redux/action/index";

import {
  DELETE_ENCODE_PLUGIN_API,
  UPLOAD_ENCODE_PLUGIN_API
} from '../ResourceManage/api/index'



// 链接reducer和action
@connect(
  createStructuredSelector({
    EncodePluginManageData: EncodePluginManageModel(),
    ProjectSummaryData: ProjectSummaryModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    setEncodePluginParam: param => dispatch(setEncodePluginParam(param)),
    searchEncodePlugin: param => dispatch(searchEncodePlugin.request(param)),

    searchProject: param => dispatch(searchProject.request(param)),
  })
)
export default class EncodePluginWrapper extends Component {
  constructor (props) {
    super(props)
    this.initParam = {
      pageNum: 1,
      pageSize: 10
    }
    this.state = {
      modalVisible: false,
      modalKey: 'modal',
    }
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillMount () {
    this.handleSearch(this.initParam, true)
  }

  handleSearch = (param, boolean) => {
    const {EncodePluginManageData} = this.props
    const {encodePluginParam} = EncodePluginManageData
    const {setEncodePluginParam, searchEncodePlugin} = this.props
    if (param) {
      searchEncodePlugin(param)
    } else {
      searchEncodePlugin(encodePluginParam)
    }
    if (boolean) {
      setEncodePluginParam(param)
    }
  }

  handlePagination = page => {
    const {EncodePluginManageData} = this.props
    const {encodePluginParam} = EncodePluginManageData
    // 分页查询并存储参数
    this.handleSearch({...encodePluginParam, pageNum: page})
  }

  handleShowSizeChange = (current, size) => {
    const {EncodePluginManageData} = this.props
    const {encodePluginParam} = EncodePluginManageData
    // 分页查询并存储参数
    this.handleSearch({...encodePluginParam, pageNum: current, pageSize: size})
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
    this.handleSearch()
  }

  handleDelete = record => {
    Request(`${DELETE_ENCODE_PLUGIN_API}/${record.id}`, {
      method: 'get'
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success(res.message)
          this.handleSearch()
        } else {
          res.message ? message.warn(res.message) : message.warn('删除失败')
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : error.message
          ?
          message.error(error.message)
          : message.error('删除失败')
      })
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
        path: '/resource-manage/encode-plugin-manager',
        name: '脱敏插件管理'
      }
    ]
    const {EncodePluginManageData} = this.props
    const {encodePlugins} = EncodePluginManageData

    const {modalVisible, modalKey} = this.state
    const projectList = Object.values(this.props.ProjectSummaryData.projectList.result)

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
        <EncodePluginSearch
          onOpenUploadModal={this.handleOpenUploadModal}
        />
        <EncodePluginGrid
          encodePlugins={encodePlugins}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onDelete={this.handleDelete}
        />
        <EncodePluginUploadModal
          key={modalKey}
          visible={modalVisible}
          api={UPLOAD_ENCODE_PLUGIN_API}
          projectList={projectList}
          onClose={this.handleCloseUploadModal}
        />
      </div>
    )
  }
}
EncodePluginWrapper.propTypes = {
  locale: PropTypes.any
}
