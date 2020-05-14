import OperatingButton from '@/app/components/common/OperatingButton'
import React, {Component} from 'react'
import {message, Popconfirm, Table, Tag, Tooltip} from 'antd'
import {FormattedMessage} from 'react-intl'
import {DELETE_SINKER_TOPOLOGY_API, RELOAD_SINKER_TOPOLOGY_API} from '@/app/containers/SinkManage/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'

export default class SinkerTopologyManageGrid extends Component {
  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`
  /**
   * @param page  传入的跳转页码  type:[Object Number]
   * @description table分页
   */
  handlePagation = page => {
    const {onPagination} = this.props
    onPagination(page)
  }

  handleDelete = id => {
    const {onSearch, sinkerParams} = this.props
    const requestAPI = `${DELETE_SINKER_TOPOLOGY_API}/${id}`
    Request(requestAPI)
      .then(res => {
        if (res && res.status === 0) {
          onSearch(sinkerParams)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({loading: false})
      })
  }

  handleReload = record => {
    Request(RELOAD_SINKER_TOPOLOGY_API, {
      data: record,
      method: 'post'
    })
      .then(res => {
        if (res && res.status === 0) {
          onSearch(sinkerParams)
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
        this.setState({loading: false})
      })
  }

  handleModify = record => {
    const {
      onOpenModifyModal
    } = this.props
    onOpenModifyModal(Object.assign({}, record))
  }

  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  /**
   * @description table默认的render
   */
  renderNomal = (text, record, index) => (
    <Tooltip title={text}>
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )

  renderStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'new':
        color = 'blue'
        break
      case 'changed':
        color = 'orange'
        break
      case 'running':
        color = 'green'
        break
      case 'stopped':
        color = 'red'
        break
      default:
        color = '#929292'
    }
    return (<div title={text} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text}
      </Tag>
    </div>)
  }

  handleValidateError = (value, record) => {
    const {onStartOrStopTopo} = this.props
    onStartOrStopTopo(value, record)
  }

  /**
   * @description selectTable的 option render
   */
  renderOperating = (text, record, index) => {
    const {onOpenRerunModal, onOpenAddSchemaModal, onOpenModifyModal, onOpenLogModal} = this.props
    let menus = [
      {
        text: <FormattedMessage
          id="app.common.modify"
          defaultMessage="修改"
        />,
        icon: 'edit',
        onClick: () => onOpenModifyModal(record)
      },
      {
        text: <FormattedMessage
          id="app.common.delete"
          defaultMessage="删除"
        />,
        icon: 'delete',
        onClick: () => this.handleDelete(record.id),
        disabled: record.status === 'running' || record.status === 'changed',
        confirmText: <div>
          <FormattedMessage
            id="app.common.delete"
            defaultMessage="删除"
          />?
        </div>
      },
      {
        text: <FormattedMessage
          id="app.common.reload"
          defaultMessage="发送reload消息"
        />,
        icon: 'reload',
        disabled: record.status !== 'running' && record.status !== 'changed',
        onClick: () => this.handleReload(record),
        confirmText: <div>
          <FormattedMessage
            id="app.common.reload"
            defaultMessage="发送reload消息"
          />?
        </div>
      },
      {
        text: <FormattedMessage
          id="app.components.projectManage.projectTopology.table.rerun"
          defaultMessage="拖回重跑"
        />,
        icon: 'export',
        disabled: record.status !== 'running' && record.status !== 'changed',
        onClick: () => onOpenRerunModal(record),
      }
    ]
    return (
      <div>
        <OperatingButton icon="plus" onClick={() => onOpenAddSchemaModal(record)}>
          <FormattedMessage
            id="app.common.addSchema"
            defaultMessage="添加schema"
          />
        </OperatingButton>
        {record.status === 'stopped' || record.status === 'new' ? (
          <Popconfirm title={<span>
            <FormattedMessage
              id="app.components.resourceManage.dataTable.start"
              defaultMessage="启动"
            />?
          </span>} onConfirm={() => this.handleValidateError('start', record)} okText="Yes" cancelText="No">
            <OperatingButton icon="caret-right">
              <FormattedMessage
                id="app.components.resourceManage.dataTable.start"
                defaultMessage="启动"
              />
            </OperatingButton>
          </Popconfirm>
        ) : (
          <Popconfirm title={<span>
            <FormattedMessage
              id="app.components.resourceManage.dataTable.stop"
              defaultMessage="停止"
            />?
          </span>} onConfirm={() => this.handleValidateError('stop', record)} okText="Yes" cancelText="No">
            <OperatingButton icon="pause">
              <FormattedMessage
                id="app.components.resourceManage.dataTable.stop"
                defaultMessage="停止"
              />
            </OperatingButton>
          </Popconfirm>
        )}
        <OperatingButton icon="file-text" onClick={() => onOpenLogModal(record)}>
          <FormattedMessage
            id="app.components.resourceManage.dataSource.viewLog"
            defaultMessage="查看日志"
          />
        </OperatingButton>
        <OperatingButton icon="ellipsis" menus={menus}/>
      </div>
    )
  }

  render() {
    const {sinkerList, tableWidth} = this.props
    const list = sinkerList && sinkerList.list
    const pagination = {
      showQuickJumper: true,
      current: (sinkerList && sinkerList.pageNum) || 1,
      pageSize: (sinkerList && sinkerList.pageSize) || 10,
      total: sinkerList && sinkerList.total,
      onChange: this.handlePagation
    }
    const columns = [
      {
        title: <FormattedMessage
          id="app.components.sinkManage.sinkerTopo.id"
          defaultMessage="ID"
        />,
        width: tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.sinkManage.sinkerTopo.sinkerName"
          defaultMessage="Sinker名称"
        />,
        width: tableWidth[1],
        dataIndex: 'sinkerName',
        key: 'sinkerName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.status"
          defaultMessage="状态"
        />,
        width: tableWidth[2],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectTopology.table.jarName"
          defaultMessage="Jar包"
        />,
        width: tableWidth[3],
        dataIndex: 'jarPath',
        key: 'jarPath',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.description"
          defaultMessage="描述"
        />,
        width: tableWidth[4],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.operate"
          defaultMessage="操作"
        />,
        width: tableWidth[5],
        render: this.renderComponent(this.renderOperating)
      }
    ]
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={list}
          columns={columns}
          pagination={pagination}
        />
      </div>
    )
  }
}

SinkerTopologyManageGrid.propTypes = {}
