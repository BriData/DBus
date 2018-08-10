/**
 * @author 戎晓伟
 * @description  普通用户-项目管理-用户管理
 */

import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
// 导入自定义组件
import { UserGrid, UserFormGrid, UserSearch, ProjectHome, Bread } from '@/app/components'

// selectors
import { makeSelectLocale } from '../LanguageProvider/selectors'
import {ProjectHomeModel} from '@/app/containers/ProjectManage/selectors'
// action
import {

} from './redux'
// 修改项目
import {
  getProjectInfo
} from '@/app/containers/ProjectManage/redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
    projectHomeData: ProjectHomeModel()
  }),
  dispatch => ({
    getProjectInfo: param => dispatch(getProjectInfo.request(param))
  })
)
export default class UserWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      projectId: '',
      modalStatus: 'create',
      modalVisibal: false,
      projectModalKey: '00000000000'
    }
    this.tableWidth = [
      '20%',
      '30%',
      '20%',
      '30%'
    ]
  }
  componentWillMount = () => {
    const { location} = this.props
    const projectId = location.query.projectId
    if (projectId) {
      this.setState({projectId})
      this.handleSearch({ id: projectId })
    } else {
      window.location.href = '/project-manage/home'
    }
  };
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params) => {
    const { getProjectInfo } = this.props
    // 查询
    getProjectInfo(params)
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
   * 新建和编辑弹窗是否显示
   */
  stateModalVisibal = (modalVisibal, modalStatus = 'create') => {
    this.setState({
      modalVisibal,
      modalStatus,
      projectModalKey: this.handleRandom('project')
    })
  };
  /**
   * @description 重新查询 用户列表
   */
  handleSearchUserList=() => {
    const { location } = this.props
    const projectId = location.query.projectId
    this.handleSearch({id: projectId})
  }

  /**
   * @param params type:[object Object]
   * @description 修改项目
   */
  handleModifyUser = id => {
    const { getProjectInfo } = this.props
    getProjectInfo({ id })
    this.stateModalVisibal(true, 'modify')
  };

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
        name: '项目用户'
      }
    ]
    this.props.location.query.projectDisplayName && breadSource.push({
      name: this.props.location.query.projectDisplayName
    })
    const { modalStatus, projectModalKey, modalVisibal, projectId } = this.state
    const {projectHomeData} = this.props
    const {projectInfo} = projectHomeData
    const {users} = projectInfo.result

    return (
      <div>
        <Helmet
          title="DbusKeeper"
          meta={[{ name: 'description', content: 'Description of Project' }]}
        />
        <Bread source={breadSource} />
        <UserSearch
          onModifyUser={this.handleModifyUser}
          projectId={projectId}
        />
        <UserGrid
          userList={users}
          tableWidth={this.tableWidth}
         />
        <ProjectHome
          initTab="user"
          isUserRole
          modalKey={projectModalKey}
          modalVisibal={modalVisibal}
          modalStatus={modalStatus}
          onCloseModal={this.stateModalVisibal}
          onSearchProject={this.handleSearchUserList}
        />
      </div>
    )
  }
}
