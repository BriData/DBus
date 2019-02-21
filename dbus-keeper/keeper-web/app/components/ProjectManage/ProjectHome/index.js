/**
 * @author 戎晓伟
 * @description  项目管理
 */

import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import Request from '@/app/utils/request'
import { Modal, message, Spin } from 'antd'
// API
import {
  ADD_PROJECT_API,
  MODIFY_PROJECT_API
} from '@/app/containers/ProjectManage/api'

// 导入自定义组件
import ProjectEditedTabs from './ProjectEditedTabs'

// selectors
import { ProjectHomeModel } from '@/app/containers/ProjectManage/selectors'
import { makeSelectLocale } from '@/app/containers/LanguageProvider/selectors'
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
  searchEncode,
  getEncodeTypeList
} from '@/app/containers/ProjectManage/redux'

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectHomeData: ProjectHomeModel(),
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
    searchResource: param => dispatch(searchResource.request(param)),
    searchEncode: param => dispatch(searchEncode.request(param)),
    getEncodeTypeList: param => dispatch(getEncodeTypeList.request(param))
  })
)
export default class ProjectHome extends Component {
  constructor (props) {
    super(props)
    this.state = {
      modalLoading: false,
      modalActiveTab: this.props.initTab || 'basic',
      errorFlag: ''
    }
    this.modalWidth = 1000
  }

  /**
   * 新建和编辑弹窗是否显示
   */
  stateModalVisibal = (modalVisibal) => {
    const {onCloseModal} = this.props
    onCloseModal(modalVisibal)
    if (!modalVisibal) {
      // 关闭窗口时清空数据
      this.handleClearModalData()
      // 默认 弹出tab
      this.handleChangeTabs(this.props.initTab || 'basic')
    }
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
   * 弹窗Loading
   */
  stateModalLoading = modalLoading => {
    this.setState({ modalLoading })
  };

  /**
   * 新增或者修改数据
   */
  handleSubmit = (params, status) => {
    const {onSearchProject} = this.props
    let Api = status === 'modify' ? MODIFY_PROJECT_API : ADD_PROJECT_API
    // 错误提示，校验
    const flag = this.handleRules(params)
    // 提交数据
    if (flag) {
      const newParams = this.handleCreateSubmitData(params)
      this.stateModalLoading(true)
      Request(Api, { data: newParams, method: 'post' })
        .then(res => {
          if (res && res.status === 0) {
            this.stateModalVisibal(false)
            // 重新查询项目列表
            onSearchProject && onSearchProject()
          } else {
            message.warn(res.message)
          }
          this.stateModalLoading(false)
        })
        .catch(error => {
          this.stateModalLoading(false)
          console.log(error)
          error.response && error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    }
  };
  /**
   * 清空 projectHomeData 数据
   */
  handleClearModalData = () => {
    const {
      setBasicInfo,
      setUser,
      setSink,
      setResource,
      setAlarm,
      setEncodes
    } = this.props
    // 清空基础信息
    setBasicInfo(null)
    // 清空用户信息
    setUser(null)
    // 清空Sink信息
    setSink(null)
    // 清空Resource信息
    setResource(null)
    // 清空报警信息
    setAlarm(null)
    // 清空脱敏配置
    setEncodes(null)
  };

  /**
   * 切换Modal Tabs
   */
  handleChangeTabs = modalActiveTab => {
    this.setState({ modalActiveTab })
  };
  /**
   * 校验弹出内容
   */
  handleRules = params => {
    let basicFlag, alarmFlag
    this.refs.projectModal &&
      this.refs.projectModal.refs.basicFormRef &&
      this.refs.projectModal.refs.basicFormRef.validateFields(err => {
        if (err) {
          basicFlag = true
        } else {
          basicFlag = false
        }
      })
    this.refs.projectModal &&
      this.refs.projectModal.refs.alarmFormRef &&
      this.refs.projectModal.refs.alarmFormRef.validateFields(err => {
        if (err) {
          alarmFlag = true
        } else {
          alarmFlag = false
        }
      })
    if (!params.basic || Object.keys(params.basic).length === 0 || basicFlag) {
      this.setState({ modalActiveTab: 'basic' })
      return false
    } else if (!params.user || Object.keys(params.user).length === 0) {
      this.setState({ modalActiveTab: 'user', errorFlag: 'userForm' })
      return false
    } else if (!params.sink || Object.keys(params.sink).length === 0) {
      this.setState({ modalActiveTab: 'sink', errorFlag: 'sinkForm' })
      return false
    }
    /*else if (!params.resource || Object.keys(params.resource).length === 0) {
      this.setState({ modalActiveTab: 'resource', errorFlag: 'resourceForm' })
      return false
    } */
    else if (!params.alarm || alarmFlag) {
      this.setState({ modalActiveTab: 'alarm' })
      return false
    }
    this.setState({ errorFlag: '' })
    return true
  };
  /**
   * 重新组装提交的数据
   */
  handleCreateSubmitData = params => {
    let temporaryData = {}
    let encodes, project, resources, sinks, users
    const projectId = params['basic']['id']
    // encodes
    encodes = {}
    params['encodes'] && Object.keys(params['encodes']).length > 0
      ? Object.entries(params['encodes']).forEach(item => {
        encodes[`${item[0]}`] = projectId
            ? Object.values(item[1]).map(item => ({
              ...item,
              projectId: projectId
            }))
            : Object.values(item[1])
      })
      : (encodes = null)
    temporaryData['encodes'] = encodes
    // sinks
    sinks = Object.values(params['sink']).map(
      item =>
        projectId
          ? { sinkId: item.id, projectId: projectId }
          : { sinkId: item.id }
    )
    temporaryData['sinks'] = sinks
    // resources
    resources = Object.values(params['resource']).map(
      item =>
        projectId
          ? {
            tableId: item.id,
            fullpullEnableFlag: Number(item.fullpull_enable_flag) || 0,
            projectId: projectId
          }
          : {
            tableId: item.id,
            fullpullEnableFlag: Number(item.fullpull_enable_flag) || 0
          }
    )
    temporaryData['resources'] = resources
    // users
    users = Object.values(params['user']).map(
      item =>
        projectId
          ? {
            userId: item.id,
            userLevel: item.userLevel,
            projectId: projectId
          }
          : {
            userId: item.id,
            userLevel: item.userLevel
          }
    )
    temporaryData['users'] = users
    // project
    project = JSON.parse(JSON.stringify(params['basic']))
    Object.entries(params['alarm']).forEach(item => {
      project[`${item[0]}Emails`] =
        item[1].value && item[1].value.map(item => item.email).join(',')
      project[`${item[0]}Flag`] = Number(item[1].checked)
    })
    temporaryData['project'] = project
    return temporaryData
  };

  render () {
    const {
      modalLoading,
      modalActiveTab,
      errorFlag
    } = this.state
    const {
      modalVisibal,
      isUserRole,
      modalStatus,
      modalKey,
      projectHomeData,
      setBasicInfo,
      setUser,
      setSink,
      setResource,
      setAlarm,
      setEncodes,
      locale,
      setUserParams,
      setSinkParams,
      setResourceParams,
      searchUser,
      searchSink,
      searchResource,
      searchEncode,
      getEncodeTypeList,
    } = this.props
    const { projectStorage, projectInfo } = projectHomeData
    return (
      <Modal
        key={modalKey}
        className="tabs-modal modal-min-height"
        visible={modalVisibal}
        maskClosable={false}
        width={this.modalWidth}
        style={{ top: 60 }}
        onCancel={() => this.stateModalVisibal(false)}
        onOk={() => this.handleSubmit(projectStorage, modalStatus)}
        confirmLoading={modalLoading}
        title={modalStatus === 'modify' ? '修改项目' : '新增项目'}
        >
        <Spin spinning={projectInfo.loading} tip="正在加载数据中...">
          {!projectInfo.loading ? (
            <ProjectEditedTabs
              ref="projectModal"
              isUserRole={isUserRole}
              projectHomeData={projectHomeData}
              modalActiveTab={modalActiveTab}
              modalStatus={modalStatus}
              onChangeTabs={this.handleChangeTabs}
              setBasicInfo={setBasicInfo}
              setUser={setUser}
              setSink={setSink}
              setResource={setResource}
              setAlarm={setAlarm}
              setEncodes={setEncodes}
              setUserParams={setUserParams}
              setSinkParams={setSinkParams}
              setResourceParams={setResourceParams}
              searchUser={searchUser}
              searchSink={searchSink}
              searchResource={searchResource}
              searchEncode={searchEncode}
              getEncodeTypeList={getEncodeTypeList}
              locale={locale}
              errorFlag={errorFlag}
              />
            ) : (
              <div style={{ height: '378px' }} />
            )}
        </Spin>
      </Modal>
    )
  }
}
ProjectHome.propTypes = {
  locale: PropTypes.any,
  isUserRole: PropTypes.bool,
  initTab: PropTypes.string,
  projectHomeData: PropTypes.object,
  setBasicInfo: PropTypes.func,
  setUser: PropTypes.func,
  setSink: PropTypes.func,
  setResource: PropTypes.func,
  setAlarm: PropTypes.func,
  setEncodes: PropTypes.func,
  setUserParams: PropTypes.func,
  setSinkParams: PropTypes.func,
  setResourceParams: PropTypes.func,
  searchUser: PropTypes.func,
  searchSink: PropTypes.func,
  searchResource: PropTypes.func,
  searchEncode: PropTypes.func,
  getEncodeTypeList: PropTypes.func,
  onCloseModal: PropTypes.func,
  modalVisibal: PropTypes.bool,
  modalStatus: PropTypes.string,
  onSearchProject: PropTypes.func,
  modalKey: PropTypes.string
}
