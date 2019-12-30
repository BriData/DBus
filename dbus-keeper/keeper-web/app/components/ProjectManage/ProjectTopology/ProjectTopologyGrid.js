/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Popconfirm,Tooltip, Tag, Table, Button, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import { fromJS } from 'immutable'
import { message} from 'antd'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectTopologyGrid extends Component {
  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

  /**
   * @description 编辑
   */
  handleModify = record => {
    const {onModifyTopo} = this.props
    onModifyTopo(record.id)
  };

  // table render
  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

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

  renderStatus =(text, record, index) => {
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
    if (!record.alias) {
      message.warn("别名不能为空,请先配置别名")
      return false
    }
    const {onStartOrStopTopo} = this.props
    onStartOrStopTopo(value, record)
  };

  /**
   * @description selectTable的 option render
   */
  renderOperating = (text, record, index) => {
    const {onOpenRerunModal, onEffect, onStartOrStopTopo, onDelete, onOpenViewTopicModal} = this.props
    const menus = [
      {
        text: <FormattedMessage id="app.components.projectManage.projectTopology.table.viewSourceTopicList" defaultMessage="查看订阅的源Topic列表" />,
        icon: 'bars',
        onClick: () => onOpenViewTopicModal(record, 'source')
      },
      {
        text: <FormattedMessage id="app.components.projectManage.projectTopology.table.viewOutputTopicList" defaultMessage="查看输出Topic列表" />,
        icon: 'bars',
        onClick: () => onOpenViewTopicModal(record, 'output')
      },
      {
        text: <FormattedMessage
          id="app.common.delete"
          defaultMessage="删除"
        />,
        icon: 'delete',
        onClick: () => onDelete(record),
        confirmText: <span>
          <FormattedMessage
            id="app.common.delete"
            defaultMessage="删除"
          />?
        </span>
      }
    ]
    record.status === 'changed' && menus.push({
      text: <FormattedMessage id="app.components.projectManage.projectTable.active" defaultMessage="生效" />,
      icon: 'check',
      onClick: () => onEffect(record),
      confirmText: <span>
        <FormattedMessage id="app.components.projectManage.projectTable.active" defaultMessage="生效" />?
      </span>
    })
    return (
      <div>
        {record.status === 'stopped' || record.status === 'new' ? (
          <Popconfirm title={<span>
            <FormattedMessage
              id="app.components.resourceManage.dataTable.start"
              defaultMessage="启动"
            />?
          </span>} onConfirm={() => this.handleValidateError('start',record)} okText="Yes" cancelText="No">
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
          </span>} onConfirm={() => this.handleValidateError('stop',record)} okText="Yes" cancelText="No">
            <OperatingButton icon="pause">
              <FormattedMessage
                id="app.components.resourceManage.dataTable.stop"
                defaultMessage="停止"
              />
            </OperatingButton>
          </Popconfirm>
        )}
        <OperatingButton icon="edit" onClick={() => this.handleModify(record)}>
          <FormattedMessage id="app.common.modify" defaultMessage="修改" />
        </OperatingButton>
        <OperatingButton disabled={record.status !== 'running' && record.status !== 'changed'} icon="reload" onClick={() => onOpenRerunModal(record)}>
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.rerun"
            defaultMessage="拖回重跑"
          />
        </OperatingButton>
        <OperatingButton icon="ellipsis" menus={menus} />
      </div>
    )
  };
  render () {
    const {
      tableWidth,
      topologyList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading, result } = topologyList
    const { total, pageSize, pageNum, list } = result.topos || {}
    const dataSource = list || []
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.project"
            defaultMessage="所属项目"
          />
        ),
        width: tableWidth[0],
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.topologyName"
            defaultMessage="拓扑名称"
          />
        ),
        width: tableWidth[1],
        dataIndex: 'topoName',
        key: 'topoName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.alias"
            defaultMessage="别名"
          />
        ),
        width: tableWidth[1],
        dataIndex: 'alias',
        key: 'alias',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.jarVersion"
            defaultMessage="Jar版本"
          />
        ),
        width: tableWidth[2],
        dataIndex: 'jarVersion',
        key: 'jarVersion',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTopology.table.jarName"
            defaultMessage="Jar包"
          />
        ),
        width: tableWidth[3],
        dataIndex: 'jarFilePath',
        key: 'jarFilePath',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.status" defaultMessage="状态" />
        ),
        width: tableWidth[4],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage id="app.common.user.backup" defaultMessage="备注" />
        ),
        width: tableWidth[5],
        dataIndex: 'topoComment',
        key: 'topoComment',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: tableWidth[6],
        render: this.renderComponent(this.renderOperating)
      }
    ]
    const pagination = {
      showSizeChanger: true,
      showQuickJumper: true,
      pageSizeOptions: ['10', '20', '50', '100'],
      current: pageNum || 1,
      pageSize: pageSize || 10,
      total: total,
      onChange: onPagination,
      onShowSizeChange: onShowSizeChange
    }
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => `${record.id}`}
          dataSource={dataSource}
          columns={columns}
          pagination={pagination}
          loading={loading}
        />
      </div>
    )
  }
}

ProjectTopologyGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  topologyList: PropTypes.object,
  onModifyTopo: PropTypes.func,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
