import React, { PropTypes, Component } from 'react'
import { Popconfirm ,InputNumber, Modal, Form, Select,message, Input, Button, Table , Row, Col} from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton/index'
import DataSourceManageStartTopoModal from './DataSourceManageStartTopoModal'
import DataSourceManageZkConfigModal from './DataSourceManageZkConfigModal'
import DataSourceManageViewLogModal from './DataSourceManageViewLogModal'
// 导入样式
import styles from '../res/styles/index.less'
import Request from "@/app/utils/request"
import {LOAD_ZK_TREE_BY_DSNAME_API} from "@/app/containers/ConfigManage/api";
import {CLONE_CONF_FROM_TEMPLATE_API,VIEW_LOG_API} from '@/app/containers/ResourceManage/api'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class DataSourceManageTopologyModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
      killWaitTime: 5,
      startModalKey: 'startModalKey',
      startModalVisible: false,
      startModalDataSource: {},
      startModalRecord: {},

      configModalKey: 'configModalKey',
      configModalVisible: false,
      configModalTree: [],

      logModalKey: 'logModalKey',
      logModalVisible: false,
      logModalRecord: {},
      logModalContent: {},
      logModalLoading: null,
    }
    this.interval = null
    this.tableWidth = [
      '20%',
      '15%',
      '150px'
    ]
  }

  componentWillMount = () => {
    this.interval = setInterval(() => {
      const {visible, onRefresh} = this.props
      visible && onRefresh()
    }, 5000)
  }

  componentWillUnmount = () => {
    this.interval && clearInterval(this.interval)
  }

  handleResetConfigModal = () => {
    const {id} = this.props
    let {dataSourceList} = this.props
    let list = dataSourceList.result && dataSourceList.result.list
    list = list && list.filter(ds => ds.id === id)
    const record = list && list[0]
    Request(CLONE_CONF_FROM_TEMPLATE_API, {
      params: {dsName: record.name, dsType: record.type}
    })
      .then(res => {
        if (res && res.status === 0) {
          message.success('重置成功！')
          this.handleOpenConfigModal()
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleOpenConfigModal = () => {
    const {readZkData} = this.props
    readZkData({path: '/'})

    const {id} = this.props
    let {dataSourceList} = this.props
    let list = dataSourceList.result && dataSourceList.result.list
    list = list && list.filter(ds => ds.id === id)
    const record = list && list[0]
    this.setState({
      configModalKey: this.handleRandom('configModalKey'),
      configModalVisible: true,
    })

    Request(LOAD_ZK_TREE_BY_DSNAME_API, {
      params: {dsName: record.name, dsType: record.type}
    })
      .then(res => {
        if (res && res.status === 0) {
          this.setState({
            configModalTree: [res.payload]
          })
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })

  }

  handleCloseConfigModal = () => {
    this.setState({
      configModalKey: this.handleRandom('configModalKey'),
      configModalVisible: false,
    })
  }

  handleOpenLogModal = (record) => {
    this.setState({
      logModalVisible: true,
      logModalRecord: record,
      logModalKey: this.handleRandom('logModalKey')
    })
    this.handleReadLog(record)
  }

  handleReadLog = record => {
    this.setState({logModalLoading: true})
    Request(VIEW_LOG_API, {
      params: {topologyId: record.topologyId}
    })
      .then(res => {
        this.setState({logModalLoading: false})
        if (res && res.status === 0) {
          this.setState({logModalContent: res.payload})
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        this.setState({logModalLoading: false})
        error.response && error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  }

  handleCloseLogModal = () => {
    this.setState({
      logModalVisible: false,
      logModalKey: this.handleRandom('logModalKey')
    })
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  renderOperating = (text, record, index) => {
    return (
      <div>
        {record.uptime === 'Not Online.' ? (
          <OperatingButton icon="caret-right" onClick={() => this.handleOpenStartModal(record)}>
            <FormattedMessage
              id="app.components.resourceManage.dataTable.start"
              defaultMessage="启动"
            />
          </OperatingButton>
        ) : (
          <Popconfirm placement="bottom" title="确定Kill？" onConfirm={() => this.handleKill(record)} okText="Yes" cancelText="No">
            <OperatingButton icon="pause">
              <FormattedMessage
                id="app.components.resourceManage.dataTable.stop"
                defaultMessage="停止"
              />
            </OperatingButton>
          </Popconfirm>

        )}
        <OperatingButton icon="file-text" onClick={() => this.handleOpenLogModal(record)}>
          <FormattedMessage
            id="app.components.resourceManage.dataSource.viewLog"
            defaultMessage="查看日志"
          />
        </OperatingButton>
        <OperatingButton icon="edit" onClick={this.handleOpenConfigModal}>
          <FormattedMessage
            id="app.components.resourceManage.dataSource.editConfig"
            defaultMessage="修改配置"
          />
        </OperatingButton>
      </div>
    )
  }

  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleOpenStartModal = (record) => {
    const {dataSourceList, id} = this.props
    let list = dataSourceList.result && dataSourceList.result.list
    list = list && list.filter(ds => ds.id === id)
    const dataSource = list && list[0]
    this.setState({
      startModalKey: this.handleRandom('startModalKey'),
      startModalVisible: true,
      startModalDataSource: dataSource,
      startModalRecord: record
    })

    const {searchJarInfos} = this.props
    searchJarInfos({
      category: 'normal',
      version: '',
      type: ''
    })
  }

  handleCloseStartModal = () => {
    this.setState({
      startModalKey: this.handleRandom('startModalKey'),
      startModalVisible: false,
    })
  }

  handleKill = record => {
    const {onKill} = this.props
    const {killWaitTime} = this.state
    onKill({
      topologyId: record.topologyId,
      waitTime: killWaitTime
    })
  }

  render () {
    const {onClose, key, visible, onRefresh, id, jarInfos} = this.props
    let {dataSourceList} = this.props
    let list = dataSourceList.result && dataSourceList.result.list
    list = list && list.filter(ds => ds.id === id)
    const record = list && list[0]
    let toposOfDs = record && record.toposOfDs
    toposOfDs = toposOfDs && Object.values(toposOfDs)

    const {topoJarStartApi} = this.props
    const {startModalKey, startModalVisible, startModalDataSource, startModalRecord} = this.state

    const {configModalKey, configModalVisible, configModalTree} = this.state
    const {readZkData, saveZkData, zkData} = this.props

    const {logModalKey, logModalRecord, logModalVisible, logModalContent, logModalLoading} = this.state
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.topoName"
            defaultMessage="拓扑名称"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'topologyName',
        key: 'topologyName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.status"
            defaultMessage="状态"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'uptime',
        key: 'uptime',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.operate"
            defaultMessage="操作"
          />
        ),
        width: this.tableWidth[2],
        key: 'operation',
        render: this.renderComponent(this.renderOperating)
      },
    ]
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={
            <div>
              <Row>
                <Col span={6}>
                  <span>
                    <FormattedMessage
                      id="app.components.resourceManage.dataSource.topologyManage"
                      defaultMessage="Topology管理"
                    />
                  </span>
                </Col>
                <Col offset={18}>
                <span>
                  <FormattedMessage
                    id="app.components.resourceManage.dataSource.killWaitTime"
                    defaultMessage="Kill等待时间"
                  />:
                </span>
                <InputNumber
                  style={{marginLeft: '5px'}}
                  min={1}
                  max={60}
                  formatter={value => `${value}s`}
                  parser={value => value.replace('s', '')}
                  defaultValue={this.state.killWaitTime}
                  onChange={value => this.setState({killWaitTime: value})}
                />
                </Col>
              </Row>
            </div>
          }
          onCancel={onClose}
          footer={[
            <Button
              type="primary"
              onClick={onRefresh}
              style={{marginLeft: '5px'}}>
              <FormattedMessage
                id="app.components.resourceManage.dataSource.forceRefresh"
                defaultMessage="强制刷新（自动刷新5秒一次）"
              />
            </Button>,
            <Button type="primary" onClick={onClose}>
              <FormattedMessage
                id="app.common.back"
                defaultMessage="返回"
              />
            </Button>]}
        >
          <Table
            rowKey='topologyName'
            dataSource={toposOfDs}
            columns={columns}
            pagination={false}
          />
        </Modal>
        <DataSourceManageStartTopoModal
          key={startModalKey}
          visible={startModalVisible}
          dataSource={startModalDataSource}
          record={startModalRecord}
          onClose={this.handleCloseStartModal}
          jarInfos={jarInfos}
          topoJarStartApi={topoJarStartApi}
        />
        <DataSourceManageZkConfigModal
          key={configModalKey}
          visible={configModalVisible}
          tree={configModalTree}
          onClose={this.handleCloseConfigModal}
          onReset={this.handleResetConfigModal}
          readZkData={readZkData}
          saveZkData={saveZkData}
          zkData={zkData}
          dataSource={record}
        />
        <DataSourceManageViewLogModal
          key={logModalKey}
          visible={logModalVisible}
          record={logModalRecord}
          content={logModalContent}
          loading={logModalLoading}
          onClose={this.handleCloseLogModal}
          onRefresh={this.handleReadLog}
        />
      </div>
    )
  }
}

DataSourceManageTopologyModal.propTypes = {
}
