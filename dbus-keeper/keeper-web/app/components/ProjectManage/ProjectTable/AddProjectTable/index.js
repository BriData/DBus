/**
 * @author 戎晓伟
 * @description  项目管理
 */
import { FormattedMessage } from 'react-intl'
import React, { PropTypes, Component } from 'react'
import { Modal, message, Spin } from 'antd'
import Request from '@/app/utils/request'
// API
import { ADD_TABLE_API, MODIFY_TABLE_API } from '@/app/containers/ProjectManage/api'

// 导入自定义组件
import ProjectTableTabs from './ProjectTableTabs'

export default class AddProjectTable extends Component {
  constructor (props) {
    super(props)
    this.state = {
      modalLoading: false
    }
    this.modalWidth = 1000
  }
  /**
   * 新建和编辑弹窗是否显示
   */
  stateModalVisibal = modalVisibal => {
    const { onCloseModal } = this.props
    onCloseModal(modalVisibal)
    if (!modalVisibal) {
      // 关闭窗口时清空数据
      this.handleClearModalData()
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
   * 新增或者修改数据
   */
  handleSubmit = (params, status) => {
    let Api = status === 'modify' ? MODIFY_TABLE_API : ADD_TABLE_API
    // 错误提示，校验
    const flag = this.handleRules(params)
    // 提交数据
    if (flag) {
      const newParams = this.handleCreateSubmitData(params)
      this.stateModalLoading(true)
      const project = this.props.projectInfo.result.project
      if (project) {
        const projectName = project.projectName
        if (!newParams.outputTopic) {
          message.error('请输入outputTopic')
          this.stateModalLoading(false)
          return
        }
        if (projectName && newParams.outputTopic.indexOf(projectName + '.') < 0)
          newParams.outputTopic = projectName + '.' + newParams.outputTopic
      }
      Request(Api, { data: newParams, method: 'post' })
        .then(res => {
          if (res && res.status === 0) {
            this.stateModalVisibal(false)
            // 重新查询项目列表
            this.props.onReloadSearch()
            if (status === 'modify' && this.props.modifyRecord.schemaChangeFlag) {
              this.props.onReload(this.props.modifyRecord)
            }
          } else {
            message.warn(res.message)
          }
          this.stateModalLoading(false)
        })
        .catch(error => {
          error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
          this.stateModalLoading(false)
        })
    }
  };
  /**
   * 弹窗Loading
   */
  stateModalLoading = modalLoading => {
    this.setState({ modalLoading })
  };
  /**
   * 校验弹出内容
   */
  handleRules = params => {
    const resource = params && params.resource && Object.keys(params.resource).length
    if (!resource) {
      message.error('没有选择任何表！')
      return false
    }
    return true
  };
  /**
   * 重新组装提交的数据
   */
  handleCreateSubmitData = params => {
    let temporaryData
    let encodes
    const { projectTableData } = this.props
    const { sinkList, topicList, topologyList } = projectTableData
    temporaryData = params['sink'] || {
      outputTopic: Object.values(topicList.result)[0],
      outputType: 'json',
      sinkId: Object.values(sinkList.result)[0].id,
    }
    // encodes
    encodes = {}
    // 需要把添加的表列出来，可以没有脱敏信息，后台会自动添加DBA脱敏
    Object.keys(params.resource).forEach(_tid => {
      encodes[_tid.substr(1)] = null
    })
    params['encodes'] && Object.keys(params['encodes']).length > 0
    && Object.entries(params['encodes']).forEach(item => {
      Object.keys(item[1].encodeOutputColumns).forEach(key => {
        item[1].encodeOutputColumns[key].fieldType = item[1].encodeOutputColumns[key].dataType
      })
      encodes[`${item[0]}`] = {
        encodeOutputColumns: Object.values(item[1].encodeOutputColumns),
        outputListType: item[1].outputListType
      }
    })
    temporaryData['projectId'] = this.props.projectId
    temporaryData['encodes'] = encodes
    temporaryData['topoId'] =
      (params['topology'] && params['topology'].topoId) ||
      Object.values(topologyList.result)[0].topoId
    return temporaryData
  };

  /**
   * 清空 projectHomeData 数据
   */
  handleClearModalData = () => {
    const {
      onSetSink,
      onSetResource,
      onSetTopology,
      onSetEncodes,
      onSelectAllResource
    } = this.props
    // 清空Sink信息
    onSetSink(null)
    // 清空Resource信息
    onSetResource(null)
    // 清空Topology
    onSetTopology(null)
    // 清空脱敏配置
    onSetEncodes(null)
    // 清空选择所有资源
    onSelectAllResource(null)
  };

  render () {
    const {
      locale,
      projectId,
      tableId,
      projectTableData,
      onSetResourceParams,
      onSetSink,
      onSetResource,
      onSetTopology,
      onSelectAllResource,
      onSetEncodes,
      onGetResourceList,
      onGetColumns,
      onGetTopologyList,
      encodeTypeList,
      onGetEncodeTypeList,
      onGetTableSinks,
      onGetTableTopics,
      onGetTableProjectAllTopo,
      getProjectInfo,
      projectInfo,
      modalVisibal,
      modalStatus,
      modalKey
    } = this.props
    const { modalLoading } = this.state
    const { projectTableStorage, tableInfo } = projectTableData
    return (
      <Modal
        key={modalKey}
        className="tabs-modal modal-min-height"
        visible={modalVisibal}
        maskClosable={false}
        confirmLoading={modalLoading}
        width={this.modalWidth}
        style={{ top: 60 }}
        title={modalStatus === 'modify' ? <FormattedMessage
          id="app.common.modify"
          defaultMessage="修改"
        /> : <FormattedMessage
          id="app.common.added"
          defaultMessage="新增"
        />}
        onCancel={() => this.stateModalVisibal(false)}
        onOk={() => this.handleSubmit(projectTableStorage, modalStatus)}
      >
        <Spin spinning={tableInfo.loading} tip="正在加载数据中...">
          {!tableInfo.loading ? (
            <ProjectTableTabs
              locale={locale}
              projectId={projectId}
              getProjectInfo={getProjectInfo}
              projectInfo={projectInfo}
              tableId={modalStatus === 'modify' ? tableId : null}
              modalStatus={modalStatus}
              projectTableData={projectTableData}
              encodeTypeList={encodeTypeList}
              onSetSink={onSetSink}
              onGetResourceList={onGetResourceList}
              onGetColumns={onGetColumns}
              onGetTopologyList={onGetTopologyList}
              onGetEncodeTypeList={onGetEncodeTypeList}
              onSetResourceParams={onSetResourceParams}
              onSetResource={onSetResource}
              onSetTopology={onSetTopology}
              onSelectAllResource={onSelectAllResource}
              onSetEncodes={onSetEncodes}
              onGetTableSinks={onGetTableSinks}
              onGetTableTopics={onGetTableTopics}
              onGetTableProjectAllTopo={onGetTableProjectAllTopo}
            />
          ) : (
            <div style={{ height: '378px' }} />
          )}
        </Spin>
      </Modal>
    )
  }
}
AddProjectTable.propTypes = {
  locale: PropTypes.any,
  projectId: PropTypes.string,
  modalVisibal: PropTypes.bool,
  modalStatus: PropTypes.string,
  modalKey: PropTypes.string,
  projectTableData: PropTypes.object,
  encodeTypeList: PropTypes.object,
  onSetResourceParams: PropTypes.func,
  onSetSink: PropTypes.func,
  onSetResource: PropTypes.func,
  onSetTopology: PropTypes.func,
  onSetEncodes: PropTypes.func,
  onGetResourceList: PropTypes.func,
  onGetColumns: PropTypes.func,
  onGetTopologyList: PropTypes.func,
  onGetEncodeTypeList: PropTypes.func,
  onGetTableSinks: PropTypes.func,
  onGetTableTopics: PropTypes.func,
  onGetTableProjectAllTopo: PropTypes.func,
  onReloadSearch: PropTypes.func,
  onCloseModal: PropTypes.func
}
