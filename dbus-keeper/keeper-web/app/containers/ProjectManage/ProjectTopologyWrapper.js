import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import { message} from 'antd'
import Helmet from 'react-helmet'
import SockJs from 'sockjs-client'
import SockJsClient from 'react-stomp';
// 导入自定义组件
import {
  ProjectTopologyGrid,
  ProjectTopologySearch,
  Bread,
  ProjectTopologyForm,
  ProjectTopologyStartModal,
  ProjectTopologyViewTopicModal,
  ProjectTopologyRerunModal
} from '@/app/components'
// selectors
import { ProjectTopologyModel, ProjectSummaryModel } from './selectors'
import { makeSelectLocale } from '../LanguageProvider/selectors'
// action
import {
  searchProject,
  searchAllTopology,
  setAllTopologyParams,
  getTopologyJarVersions,
  getTopologyJarPackages,
  getTopologyInfo,
  getTopologyFeeds,
  getTopologyOutTopic,
  topologyEffect,
  topologyRerunInit
} from './redux'

import {
  START_OR_STOP_TOPOLOGY_API,
  DELETE_TOPOLOGY_API,
  GET_TOPOLOGY_TEMPLATE_API,
  TOPOLOGY_RERUN_API
} from './api'

import Request, {setToken} from '@/app/utils/request'

// 链接reducer和action
@connect(
  createStructuredSelector({
    projectData: ProjectSummaryModel(),
    projectTopologyData: ProjectTopologyModel(),
    locale: makeSelectLocale()
  }),
  dispatch => ({
    getTopologyList: param => dispatch(searchAllTopology.request(param)),
    searchProject: param => dispatch(searchProject.request(param)),
    getTopologyInfo: param => dispatch(getTopologyInfo.request(param)),
    setTopologyParams: param => dispatch(setAllTopologyParams(param)),
    getTopologyJarVersions: param =>
      dispatch(getTopologyJarVersions.request(param)),
    getTopologyJarPackages: param =>
      dispatch(getTopologyJarPackages.request(param)),
    getTopologyFeeds: param => dispatch(getTopologyFeeds.request(param)),
    getTopologyOutTopic: param => dispatch(getTopologyOutTopic.request(param)),
    topologyEffect: param => dispatch(topologyEffect.request(param)),
    topologyRerunInit: param => dispatch(topologyRerunInit.request(param)),
  })
)
export default class ProjectTopologyWrapper extends Component {

  constructor (props) {
    super(props)
    this.state = {
      modalStatus: 'create',
      modalVisibal: false,
      modalKey: '0000000001',

      startTopoModalVisible: false,
      startTopoModalLog: '',
      startTopoModalLoading: false,

      viewTopicModalVisible: false,
      viewTopicModalTopoName: '',
      viewTopicModalType: '', // source or output

      rerunModalVisible: false,
      rerunModalKey: 'rerunModalKey',
      rerunModalRecord: {},

      date: new Date()
    }
    this.tableWidth = ['15%', '15%', '10%', '35%', '15%', '15%', '20%']
    this.initParams = {
      pageNum: 1,
      pageSize: 10
    }
    this.projectId = null
    this.websocket = null
    this.stompClient = null
  }
  componentWillMount () {
    const { searchProject, projectId } = this.props
    this.projectId = projectId
    // 初始化查询
    this.handleSearch({...this.initParams, projectId: this.projectId}, true)
    // 获取项目列表
    !this.projectId && searchProject()
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

  handleCreateTopology = () => {
    if (!this.projectId) {
      message.error('请选择项目')
      return
    }
    this.stateModalVisibal(true, 'create')
  }

  stateModalVisibal = (modalVisibal, modalStatus = 'create') => {
    this.setState({
      modalVisibal,
      modalStatus,
      modalKey: this.handleRandom('modal')
    })
  };
  /**
   * @param params 查询的参数 type:[Object Object]
   * @param boolean 是否将参数缓存起来 type:[Object Boolean]
   * @description 查询数据
   */
  handleSearch = (params, boolean) => {
    const { getTopologyList, setTopologyParams } = this.props
    // 查询
    getTopologyList(params)
    if (boolean) {
      // 存储
      setTopologyParams(params)
    }
  };
  /**
   * @param params type:[object Object]
   * @description 修改Topo
   */
  handleModifyTopo = id => {
    const { getTopologyInfo } = this.props
    getTopologyInfo({ id })
    this.stateModalVisibal(true, 'modify')
  };

  /**
   * @param page 跳转页码 type:[object Number]
   * @description 分页查询
   */
  handlePagination = page => {
    const { projectTopologyData } = this.props
    const { topologyParams } = projectTopologyData
    // 分页查询并存储参数
    this.handleSearch({ ...topologyParams, pageNum: page }, true)
  };

  /**
   * @param current 当前页码 type:[object Number]
   * @param size 每页需要展示的页数 type:[Object Number]
   * @description 切换页数查询
   */
  handleShowSizeChange = (current, size) => {
    const { projectTopologyData } = this.props
    const { topologyParams } = projectTopologyData
    this.handleSearch(
      { ...topologyParams, pageNum: current, pageSize: size },
      true
    )
  };

  sendMessage = (msg) => {
    const record = this.state.socketRecord
    const {socketTopoName, date} = this.state
    this.clientRef.sendMessage('/startOrStop', JSON.stringify({
      'message' : JSON.stringify({
        cmdType: 'start',
        topoName: record.topoName,
        id: record.id,
        jarPath: record.jarFilePath,
        uid: `${socketTopoName}${date.getTime()}`
      })
    }))
  }

  handleStartOrStopTopo = (operate, record) => {
    // this.setState({
    //   socketTopoName: record.topoName,
    //   date: new Date(),
    //   socketRecord: record
    // }, this.sendMessage)
    // const socket = new SockJs(START_OR_STOP_TOPOLOGY_API)
    // this.stompClient = Stomp.over(socket);
    // const date = new Date()
    // this.stompClient.connect({}, function(frame) {
    //   stompClient.subscribe(`/user/${record.topoName}${date.getTime()}/log`, function(r) {
    //     console.info(r)
    //   });
    // });
    // this.stompClient.send("/startOrStop", {}, JSON.stringify({
    //   'message' : JSON.stringify({
    //     cmdType: operate,
    //     topoName: record.topoName,
    //     id: record.id,
    //     jarPath: record.jarFilePath,
    //     uid: `${record.topoName}${date.getTime()}`
    //   })
    // }))
    this.setState({
      startTopoModalVisible: true,
      startTopoModalLog: `发送${operate}命令中...`,
      startTopoModalLoading: true
    })

    Request(START_OR_STOP_TOPOLOGY_API, {
      data: {
        cmdType: operate,
        topoName: record.topoName,
        projectName: record.projectName,
        id: record.id,
        jarPath: record.jarFilePath,
        alias: record.alias
      },
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            startTopoModalLog: `${res.message}`
          })
        } else {
          message.warn(res.message)
        }
        this.setState({
          startTopoModalLoading: false
        })
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({
          startTopoModalLoading: false
        })
      })
    // this.websocket = new WebSocket(START_OR_STOP_TOPOLOGY_API)
    // this.websocket.onopen = evt => {
    //   this.setState({startTopoModalVisible: true})
    //   this.websocket.send(JSON.stringify({
    //     cmdType: operate,
    //     topoName: record.topoName,
    //     id: record.id,
    //     jarPath: record.jarFilePath
    //   }))
    // }
    // this.websocket.onclose = evt => {
    //   console.info('websocket closed')
    // }
    // this.websocket.onmessage = evt => {
    //   this.setState({
    //     startTopoModalLog: `${this.state.startTopoModalLog}${evt.data}\n`
    //   })
    //   console.info(evt.data)
    // }
    // this.websocket.onerror = evt => {
    // }
  }

  handleCloseTopoModal = () => {
    this.setState({ startTopoModalVisible: false })
    this.setState({ startTopoModalLog: '' })
    // if (this.stompClient != null) {
    //   this.stompClient.disconnect();
    // }
    // if (this.websocket) this.websocket.close()
    const {projectTopologyData} = this.props
    const {topologyParams} = projectTopologyData
    this.handleSearch(topologyParams)
  }

  handleDelete = (record) => {
    const {projectTopologyData} = this.props
    const {topologyParams} = projectTopologyData
    Request(`${DELETE_TOPOLOGY_API}/${record.id}`, {method: 'get'})
      .then(res => {
        if (res && res.status === 0) {
          this.handleSearch(topologyParams)
          message.success(res.message)
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

  handleCloseViewTopicModal = () => {
    this.setState({
      viewTopicModalVisible: false
    })
  }

  handleOpenViewTopicModal = (record, type) => {
    const {getTopologyFeeds, getTopologyOutTopic} = this.props
    type === 'source' ? getTopologyFeeds(record) : getTopologyOutTopic(record)
    this.setState({
      viewTopicModalVisible: true,
      viewTopicModalTopoName: record.topoName,
      viewTopicModalType: type
    })
  }

  handleCloseRerunModal = () => {
    this.setState({
      rerunModalVisible: false,
      rerunModalKey: this.handleRandom('rerunModalKey')
    })
  }

  handleOpenRerunModal = (record) => {
    const {topologyRerunInit} = this.props
    topologyRerunInit(record)
    this.setState({
      rerunModalVisible: true,
      rerunModalRecord: record
    })
  }

  render () {
    console.info(this.props)
    const {
      locale,
      // isCreate 用来判断是否是用户角度进入此组件
      isCreate,
      projectTopologyData,
      getTopologyJarVersions,
      getTopologyJarPackages,
      projectData
    } = this.props
    const { modalStatus, modalVisibal, modalKey } = this.state
    // 启动topo 需要的state
    const { startTopoModalVisible, startTopoModalLog, startTopoModalLoading } = this.state

    // 展示topic的对话框
    const {
      viewTopicModalVisible,
      viewTopicModalTopoName,
      viewTopicModalType
    } = this.state

    const {
      rerunModalVisible,
      rerunModalKey,
      rerunModalRecord
    } = this.state
    const rerunInitResult = projectTopologyData.rerunInitResult.result.payload || []

    const {
      topologyList,
      topoInfo,
      topologyParams,
      jarVersions,
      jarPackages,
      feeds,
      outTopic
    } = projectTopologyData
    const { projectList } = projectData

    const {topologyEffect} = this.props
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
        name: 'Topology'
      }
    ]
    this.props.projectDisplayName && breadSource.push({
      name: this.props.projectDisplayName
    })
    return (
      <div>
        <Helmet
          title="ProjectTopology"
          meta={[
            { name: 'description', content: 'Description of ProjectTopology' }
          ]}
        />
        <Bread source={breadSource} />
        <ProjectTopologySearch
          locale={locale}
          isCreate={isCreate}
          projectId={this.projectId}
          onProjectIdChange={id => this.projectId = id}
          topologyParams={topologyParams}
          projectList={projectList}
          topologyList={topologyList}
          onSearch={this.handleSearch}
          onCreateTopology={this.handleCreateTopology}
        />
        <ProjectTopologyGrid
          locale={locale}
          tableWidth={this.tableWidth}
          topologyList={topologyList}
          onModifyTopo={this.handleModifyTopo}
          onPagination={this.handlePagination}
          onShowSizeChange={this.handleShowSizeChange}
          onStartOrStopTopo={this.handleStartOrStopTopo}
          onDelete={this.handleDelete}
          onOpenViewTopicModal={this.handleOpenViewTopicModal}
          onEffect={topologyEffect}
          onOpenRerunModal={this.handleOpenRerunModal}
        />
        <ProjectTopologyForm
          key={modalKey}
          modalKey={modalKey}
          locale={locale}
          modalStatus={modalStatus}
          visibal={modalVisibal}
          topologyInfo={topoInfo}
          versions={jarVersions}
          packages={jarPackages}
          topologyParams={topologyParams}
          projectId={this.projectId}
          onSearch={this.handleSearch}
          onCloseModal={this.stateModalVisibal}
          onGetJarVersions={getTopologyJarVersions}
          onGetJarPackages={getTopologyJarPackages}
          getTopologyTemplateApi={GET_TOPOLOGY_TEMPLATE_API}
        />
        <ProjectTopologyStartModal
          visible={startTopoModalVisible}
          onClose={this.handleCloseTopoModal}
          startTopoModalLog={startTopoModalLog}
          loading={startTopoModalLoading}
        />
        <ProjectTopologyViewTopicModal
          visible={viewTopicModalVisible}
          topoName={viewTopicModalTopoName}
          type={viewTopicModalType}
          onClose={this.handleCloseViewTopicModal}
          inTopic={feeds}
          outTopic={outTopic}
        />
        <ProjectTopologyRerunModal
          visible={rerunModalVisible}
          key={rerunModalKey}
          record={rerunModalRecord}
          onClose={this.handleCloseRerunModal}
          rerunInitResult={rerunInitResult}
          topologyRerunApi={TOPOLOGY_RERUN_API}
        />
        {/*<SockJsClient url='http://localhost:8902/webSocket'*/}
                      {/*topics={[`/user/${this.state.socketTopoName}${this.state.date.getTime()}/log`]}*/}
                      {/*onMessage={(msg) => { console.log(msg); }}*/}
                      {/*ref={ (client) => { this.clientRef = client }} />*/}
      </div>
    )
  }
}
ProjectTopologyWrapper.propTypes = {
  locale: PropTypes.any,
  projectTopologyData: PropTypes.object,
  projectData: PropTypes.object,
  getTopologyInfo: PropTypes.func,
  getTopologyList: PropTypes.func,
  getTopologyJarVersions: PropTypes.func,
  getTopologyJarPackages: PropTypes.func,
  setTopologyParams: PropTypes.func
}
