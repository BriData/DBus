import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
import Request, {setToken} from '@/app/utils/request'
// 导入自定义组件
import {
  Bread,
  UserKeyDownloadSearch
} from '@/app/components/index'
// selectors
import { } from '../ResourceManage/selectors/index'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
} from '../ResourceManage/redux/index'

import {
  DOWNLOAD_USER_KEY_API
} from '../ResourceManage/api/index'
import {
  GET_PROJECT_INFO_API
} from '@/app/containers/ProjectManage/api'


// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale()
  }),
  dispatch => ({
  })
)
export default class UserKeyDownloadWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: null,
      principal: null
    }
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  componentWillMount () {
    const {location} = this.props
    const projectId = location.query.projectId
    if (projectId) {
      this.setState({projectId})
      this.handleGetPrincipal(projectId)
    } else {
      window.location.href = '/project-manage/home'
    }
  }

  handleGetPrincipal = projectId => {
    Request(`${GET_PROJECT_INFO_API}/${projectId}`, {
      method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          if (res.payload) {
            this.setState({
              principal: res.payload.project.principal
            })
          }
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
  // handleSearch = (param, boolean) => {
  // }
  //
  // handlePagination = page => {
  //   const {EncodePluginManageData} = this.props
  //   const {encodePluginParam} = EncodePluginManageData
  //   // 分页查询并存储参数
  //   this.handleSearch({...encodePluginParam, pageNum: page})
  // }
  //
  // handleShowSizeChange = (current, size) => {
  //   const {EncodePluginManageData} = this.props
  //   const {encodePluginParam} = EncodePluginManageData
  //   // 分页查询并存储参数
  //   this.handleSearch({...encodePluginParam, pageNum: current, pageSize: size})
  // }

  // handleOpenUploadModal = () => {
  //   this.setState({
  //     modalVisible: true
  //   })
  // }
  //
  // handleCloseUploadModal = () => {
  //   this.setState({
  //     modalKey: this.handleRandom('modal'),
  //     modalVisible: false
  //   })
  //   // this.handleSearch()
  // }

  // handleDelete = record => {
  //   Request(`${DELETE_ENCODE_PLUGIN_API}/${record.id}`, {
  //     method: 'get'
  //   })
  //     .then(res => {
  //       if (res && res.status === 0) {
  //         message.success(res.message)
  //         this.handleSearch()
  //       } else {
  //         message.warn(res.message)
  //       }
  //     })
  //     .catch(error => {
  //       error.response.data && error.response.data.message
  //         ? message.error(error.response.data.message)
  //         : message.error(error.message)
  //     })
  // }

  handleDownload = () => {
    const TOKEN = window.localStorage.getItem('TOKEN')
    window.open(`${DOWNLOAD_USER_KEY_API}/${this.state.projectId}?token=${TOKEN}`)
  }

  render () {
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
        path: '/project-manage/user-key-download-manager',
        name: '用户密钥管理'
      }
    ]
    this.props.location.query.projectDisplayName && breadSource.push({
      name: this.props.location.query.projectDisplayName
    })
    console.info(this.props)
    const {principal} = this.state
    return (
      <div>
        <Helmet
          title="User Key"
          meta={[
            { name: 'description', content: 'Description of User Key Manage' }
          ]}
        />
        <Bread source={breadSource} />
        <UserKeyDownloadSearch
          onOpenDownload={this.handleDownload}
          principal={principal}
        />
      </div>
    )
  }
}
UserKeyDownloadWrapper.propTypes = {
  locale: PropTypes.any
}
