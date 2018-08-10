/**
 * @author 戎晓伟
 * @description  项目管理
 */

import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Request from '@/app/utils/request'
import Helmet from 'react-helmet'
import { message } from 'antd'
// API
import {
  DELETE_PROJECT_API,
  ENABLE_DISABLE_PROJECT_API
} from './api'

import { getUserInfo } from '@/app/utils/request'

// 导入自定义组件
import { ProjectSummary, ProjectHome } from '@/app/components'

// selectors
import { ProjectHomeModel, ProjectSummaryModel } from './selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  setBasicInfo,
  setUser,
  setSink,
  setResource,
  setAlarm,
  setEncodes,
  setUserParams,
  setSinkParams,
  setResourceParams,
  searchUser,
  searchSink,
  searchResource,
  searchProject,
  getProjectInfo,
  deleteProject,
  enableDisableProject,
  searchEncode,
  getEncodeTypeList
} from './redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectHomeData: ProjectHomeModel(),
    projectSummary: ProjectSummaryModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    setBasicInfo: param => dispatch(setBasicInfo(param)),
    setUser: param => dispatch(setUser(param)),
    setSink: param => dispatch(setSink(param)),
    setResource: param => dispatch(setResource(param)),
    setAlarm: param => dispatch(setAlarm(param)),
    setEncodes: param => dispatch(setEncodes(param)),
    setUserParams: param => dispatch(setUserParams(param)),
    setSinkParams: param => dispatch(setSinkParams(param)),
    setResourceParams: param => dispatch(setResourceParams(param)),
    searchUser: param => dispatch(searchUser.request(param)),
    searchSink: param => dispatch(searchSink.request(param)),
    searchProject: param => dispatch(searchProject.request(param)),
    getProjectInfo: param => dispatch(getProjectInfo.request(param)),
    deleteProject: param => dispatch(deleteProject.request(param)),
    enableDisableProject: param =>
      dispatch(enableDisableProject.request(param)),
    searchResource: param => dispatch(searchResource.request(param)),
    searchEncode: param => dispatch(searchEncode.request(param)),
    getEncodeTypeList: param => dispatch(getEncodeTypeList.request(param))
  })
)
export default class ProjectHomeWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      modalVisibal: false,
      modalStatus: 'create',
      modalKey: '00000000000000000000000'
    }
  }

  /**
   * 新建和编辑弹窗是否显示
   */
  stateModalVisibal = (modalVisibal, modalStatus = 'create') => {
    this.setState({
      modalVisibal,
      modalStatus,
      modalKey: this.handleRandom('modal')
    })
  };

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  handleEnableDisableProject = project => {
    const Api = ENABLE_DISABLE_PROJECT_API
    Request(Api, { data: project, method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          // 重新查询项目列表
          this.handleSearchProject()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  };

  // 删除项目
  handleDeleteProject = id => {
    const Api = DELETE_PROJECT_API
    Request(`${Api}/${id}`, { method: 'get' })
      .then(res => {
        if (res && res.status === 0) {
          // 重新查询项目列表
          this.handleSearchProject()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  };

  handleSearchProject = () => {
    const { searchProject } = this.props
    const userInfo = getUserInfo()
    searchProject({ __user_id: userInfo.userId, __role_type: userInfo.roleType })
  };

  handleModifyProject = id => {
    const { getProjectInfo } = this.props
    getProjectInfo({ id })
    this.stateModalVisibal(true, 'modify')
  };

  componentWillMount = () => {
    this.handleSearchProject()

  };

  render () {
    const {
      modalVisibal,
      modalStatus,
      modalKey
    } = this.state
    const {
      locale,
      projectSummary
    } = this.props
    return (
      <div>
        <Helmet
          title="Project"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <ProjectSummary
          projectSummary={projectSummary}
          onModifyProject={this.handleModifyProject}
          onEnableDisableProject={this.handleEnableDisableProject}
          onDeleteProject={this.handleDeleteProject}
          showModal={this.stateModalVisibal}
        />
        <ProjectHome
          isUserRole={getUserInfo().roleType !== 'admin'}
          modalKey={modalKey}
          modalVisibal={modalVisibal}
          modalStatus={modalStatus}
          onCloseModal={this.stateModalVisibal}
          onSearchProject={this.handleSearchProject}
        />
      </div>
    )
  }
}
ProjectHomeWrapper.propTypes = {
  locale: PropTypes.any,
  projectSummary: PropTypes.object,
  searchProject: PropTypes.func,
  getProjectInfo: PropTypes.func
}
