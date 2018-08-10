import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Helmet from 'react-helmet'
import { intlMessage } from '@/app/i18n'
// 导入自定义组件
import {
  UserManageGrid,
  UserManageSearch,
  UserForm,
  UserProject,
  ProjectHome,
  Bread
} from '@/app/components'
// selectors
import { userManageReducerModel } from './selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  setUserListParams,
  searchUserList,
  getUserInfo,
  getUserProject
} from './redux'
// 修改项目
import {
  getProjectInfo
} from '@/app/containers/ProjectManage/redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    userManageData: userManageReducerModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    getUserList: param => dispatch(searchUserList.request(param)),
    getUserInfo: param => dispatch(getUserInfo.request(param)),
    getUserProject: param => dispatch(getUserProject.request(param)),
    setUserListParams: param => dispatch(setUserListParams(param)),
    getProjectInfo: param => dispatch(getProjectInfo.request(param))
  })
)
export default class UserManageWrapper extends Component {
  constructor (props) {
    super(props)
    this.state = {
      visibal: false,
      modalStatus: 'create',
      modalVisibal: false,
      projectModalKey: '00000000000',
      userModalKey: '0000000001',
      userProjectVisibal: false
    }
    this.tableWidth = ['15%', '30%', '15%', '30%']
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
  }
  componentWillMount () {
    // 初始化查询
    this.handleSearch(this.initParams, true)
  }
  /**
   * @param visibal type:[Object Boolean]
   * @returns 关闭和显示用户弹窗
   */
  handleUserVisibal = visibal => {
    this.setState({ visibal })
    if (visibal === false) {
      this.setState({userModalKey: this.handleRandom('user')})
    }
  };

  /**
   * @param visibal type:[Object Boolean]
   * @returns 关闭和显示用户项目弹窗
   */
  handleProjectVisibal = userProjectVisibal => {
    this.setState({ userProjectVisibal })
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
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params, boolean) => {
    const { getUserList, setUserListParams } = this.props
    // 查询
    getUserList(params)
    if (boolean) {
      // 存储
      setUserListParams(params)
    }
  };
  /**
   * @param id 用户ID
   * @description 获取到用户信息并弹窗
   */
  handleGetUserInfo=(id) => {
    const {getUserInfo} = this.props
    getUserInfo({id})
    this.handleUserVisibal(true)
    this.handleChangeModalStatus('modify')
  }
  /**
   * @param id 用户ID
   * @description 获取到用户信息并弹窗
  */
  handleCreateUser=() => {
    this.handleUserVisibal(true)
    this.handleChangeModalStatus('create')
  }

  /**
   * @param modalStatus 弹窗状态
   * @description 切换弹窗状态
  */
  handleChangeModalStatus=(modalStatus) => {
    this.setState({modalStatus})
  }

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const { userManageData } = this.props
    const { userListParams } = userManageData
    // 分页查询并存储参数
    this.handleSearch({ ...userListParams, pageNum: page }, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const { userManageData } = this.props
    const { userListParams } = userManageData
    this.handleSearch(
      { ...userListParams, pageNum: current, pageSize: size },
      true
    )
  };
  /**
   * @param params type:[object Object]
   * @description 获取项目
   */
  handleSearchProject=(params) => {
    const { getUserProject } = this.props
    this.handleProjectVisibal(true)
    getUserProject(params)
  }

  /**
   * @param params type:[object Object]
   * @description 修改项目
   */
  handleModifyProject = id => {
    const { getProjectInfo } = this.props
    getProjectInfo({ id })
    this.stateModalVisibal(true, 'modify')
  };
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
  render () {
    const { visibal, modalStatus, userModalKey, projectModalKey, userProjectVisibal, modalVisibal } = this.state
    const { locale, userManageData } = this.props
    const localeMessage = intlMessage(locale)
    const {
      userList,
      userInfo,
      userProject,
      userListParams
    } = userManageData
    const breadSource = [
      {
        path: '/project-manage',
        name: 'home'
      },
      {
        name: localeMessage({ id: 'app.components.navigator.userManage' })
      }
    ]
    return (
      <div>
        <Helmet
          title="ProjectResource"
          meta={[
            { name: 'description', content: 'Description of ProjectResource' }
          ]}
        />
        <Bread source={breadSource} />
        <UserManageSearch
          locale={locale}
          onCreateUser={this.handleCreateUser}
          userListParams={userListParams}
          onSearch={this.handleSearch}
        />
        <UserManageGrid
          locale={locale}
          tableWidth={this.tableWidth}
          userList={userList}
          userListParams={userListParams}
          onGetUserInfo={this.handleGetUserInfo}
          onSearchProject={this.handleSearchProject}
          onSearch={this.handleSearch}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
        />
        <UserForm
          modalKey={userModalKey}
          locale={locale}
          modalStatus={modalStatus}
          visibal={visibal}
          userInfo={userInfo}
          userListParams={userListParams}
          onSearch={this.handleSearch}
          onCloseModal={this.handleUserVisibal}
        />
        <UserProject
          locale={locale}
          visibal={userProjectVisibal}
          onCloseModal={this.handleProjectVisibal}
          userProject={userProject}
          onModifyProject={this.handleModifyProject}
        />
        <ProjectHome
          initTab="user"
          modalKey={projectModalKey}
          modalVisibal={modalVisibal}
          modalStatus={modalStatus}
          onCloseModal={this.stateModalVisibal}
        />
      </div>
    )
  }
}
UserManageWrapper.propTypes = {
  locale: PropTypes.any,
  userManageData: PropTypes.object,
  setUserListParams: PropTypes.func,
  getUserList: PropTypes.func,
  getUserInfo: PropTypes.func,
  getUserProject: PropTypes.func,
  getProjectInfo: PropTypes.func
}
