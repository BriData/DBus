/**
 * @author 戎晓伟
 * @description  项目管理-（新建,修改组件）
 */

import React, { PropTypes, Component } from 'react'
import { FormattedMessage } from 'react-intl'
import { Tabs, Icon, Spin } from 'antd'
// 导入自定义组件
import ResourceForm from './ResourceForm'
import SinkForm from './SinkForm'
// 导入样式
import styles from '../../res/styles/index.less'

const TabPane = Tabs.TabPane
// 常量
const SELECTED_TABLE_SCROLL_Y = 157

export default class ProjectTableTabs extends Component {
  constructor (props) {
    super(props)
    this.state = {
      alarmFlag: false
    }
    this.tableWidthStyle = width => ({
      width: `${parseFloat(width) / 100 * 960 - 15}px`
    })
  }
  componentWillMount () {
    const {
      onGetTableSinks,
      projectId,
      onGetTableProjectAllTopo,
      projectTableData,
      modalStatus,
      getProjectInfo
    } = this.props
    const { tableInfo } = projectTableData
    // 存储projectId
    if (modalStatus === 'modify') {
      this.handleCreateData(JSON.parse(JSON.stringify(tableInfo.result)))
    } else {
      onGetTableSinks({ projectId, sinkId: null })
    }
    // 获取TopologyList
    onGetTableProjectAllTopo({ projectId })
    getProjectInfo({id:projectId})
  }
  /**
   * @param param  [object object] Object
   * @description 将数组重新生成一个 子项为 {key,value}的object
   */
  handleCreateArrayToMap = (param, name) => {
    const temporaryParam = {}
    param.forEach(item => {
      temporaryParam[`_${name ? item[`${name}`] : item.id}`] =
        name === 'cid'
          ? { ...item, tableId: item.tid, fieldName: item.columnName }
          : item
    })
    return temporaryParam
  };
  /**
   * @param params [object Object] 接收原始数据
   * @description 重新组装数据并存储到redux
   */
  handleCreateData = params => {
    let temporaryData = {}
    let sink, resource, topology, encodes
    const {
      onSetSink,
      onSetResource,
      onSetTopology,
      onSetEncodes,
      projectId,
      onGetTableSinks,
      tableId,
      modifyRecord
    } = this.props
    // 组装sink
    sink = {}
    sink = params['sink']
    temporaryData['sink'] = sink
    sink['sinkId'] = `${params['sink'].sinkId}`
    sink['id'] = `${tableId}`
    onGetTableSinks({ projectId, sinkId: params['sink'].sinkId })
    onSetSink(sink)
    // 组装 topology
    topology = {}
    topology['topoId'] = `${modifyRecord.topoId}`
    temporaryData['topology'] = topology
    onSetTopology(topology)
    // 组装 encodes
    encodes = {}
    params['encodes'] && Object.keys(params['encodes']).length > 0
      ? Object.entries(params['encodes']).forEach(item => {
        encodes[`${item[0]}`] = {
          encodeOutputColumns: this.handleCreateArrayToMap(
              item[1].encodeOutputColumns,
              'cid'
            ),
          outputListType: item[1].outputListType
        }
      })
      : (encodes = null)
    temporaryData['encodes'] = encodes
    onSetEncodes(encodes)
    // 组装 resource
    resource = { [`_${params['resource'].projectTableId}`]: params['resource'] }
    temporaryData['resource'] = resource
    onSetResource(resource)
  };

  render () {
    const {
      modifyRecord,
      locale,
      projectId,
      projectInfo,
      modalStatus,
      projectTableData,
      encodeTypeList,
      onSetResourceParams,
      onSetSink,
      onSetResource,
      onSetTopology,
      onSelectAllResource,
      onSetEncodes,
      onGetResourceList,
      onGetColumns,
      onGetTopologyList,
      onGetEncodeTypeList,
      onGetTableSinks,
      onGetTableTopics
    } = this.props
    const {
      projectTableStorage,
      topologyList,
      tableInfo,
      encodesList,
      columnsList,
      resourceList,
      resourceParams,
      sinkList,
      topicList,
      projectTopos
    } = projectTableData
    const { sink, resource, topology, encodes } = projectTableStorage
    return (
      <div className="project-modal-tabs">
        <Tabs defaultActiveKey="resource">
          <TabPane
            tab={
              <span>
                <Icon type="appstore-o" />
                <FormattedMessage
                  id="app.components.projectManage.projectTable.resourceChoice"
                  defaultMessage="资源选择"
                />
              </span>
            }
            key="resource"
          >
            <Spin spinning={topicList.loading}>
              {!topicList.loading ? (
                <ResourceForm
                  modifyRecord={modifyRecord}
                  locale={locale}
                  projectId={projectId}
                  resource={resource}
                  encodes={encodes}
                  modalStatus={modalStatus}
                  encodesList={encodesList}
                  encodeList={columnsList}
                  resourceList={resourceList}
                  encodeTypeList={encodeTypeList}
                  sinkList={sinkList}
                  topicList={topicList}
                  topology={topology}
                  projectToposList={projectTopos}
                  onSetTopology={onSetTopology}
                  onSelectAllResource={onSelectAllResource}
                  projectTableStorage={projectTableStorage}
                  selectedTableScrollY={SELECTED_TABLE_SCROLL_Y}
                  resourceParams={resourceParams}
                  onSearchList={onGetResourceList}
                  onSearchEncode={onGetColumns}
                  onGetEncodeTypeList={onGetEncodeTypeList}
                  onSetParams={onSetResourceParams}
                  onSetEncodes={onSetEncodes}
                  onSetSink={onSetSink}
                  tableWidthStyle={this.tableWidthStyle}
                  onSetResource={onSetResource}
            />
          ) : (
            <div style={{ height: '378px' }} />
          )}
            </Spin>
          </TabPane>
          <TabPane
            tab={
              <span>
                <Icon type="schedule" />
                <FormattedMessage
                  id="app.components.projectManage.projectTable.sinkChoice"
                  defaultMessage="Sink选择"
                />
              </span>
            }
            key="sink"
          >
            {sinkList.loaded && sink && (
              <SinkForm
                ref="sinkFormRef"
                locale={locale}
                projectId={projectId}
                projectInfo={projectInfo}
                sink={sink}
                sinkList={sinkList}
                topicList={topicList}
                onSetSink={onSetSink}
                onGetTableSinks={onGetTableSinks}
                onGetTableTopics={onGetTableTopics}
              />
            )}
          </TabPane>
        </Tabs>
      </div>
    )
  }
}

ProjectTableTabs.propTypes = {
  locale: PropTypes.any,
  modalStatus: PropTypes.string,
  projectId: PropTypes.string,
  projectTableData: PropTypes.object,
  encodeTypeList: PropTypes.object,
  onSetResourceParams: PropTypes.func,
  onSetSink: PropTypes.func,
  onSetResource: PropTypes.func,
  onSetTopology: PropTypes.func,
  onSelectAllResource: PropTypes.func,
  onSetEncodes: PropTypes.func,
  onGetResourceList: PropTypes.func,
  onGetColumns: PropTypes.func,
  onGetTopologyList: PropTypes.func,
  onGetEncodeTypeList: PropTypes.func,
  onGetTableSinks: PropTypes.func,
  onGetTableTopics: PropTypes.func,
  onGetTableProjectAllTopo: PropTypes.func
}
