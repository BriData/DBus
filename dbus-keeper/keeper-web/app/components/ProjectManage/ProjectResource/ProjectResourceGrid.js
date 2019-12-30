/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import {Tooltip, Tag, Table, Button, Input } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

export default class ProjectResourceGrid extends Component {

  constructor(props) {
    super(props)
    const {tableWidth} = props
    this.adminColumns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.project"
            defaultMessage="所属项目"
          />
        ),
        width: tableWidth[0],
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.dataSourceType"
          defaultMessage="数据源类型"
        />,
        width: tableWidth[2],
        dataIndex: 'dsType',
        key: 'dsType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.dataSourceName"
          defaultMessage="数据源名称"
        />,
        width: tableWidth[3],
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.dataSchemaName"
          defaultMessage="Schema名称"
        />,
        width: tableWidth[4],
        dataIndex: 'schemaName',
        key: 'schemaName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.resourceManage.dataTableName"
          defaultMessage="表名"
        />,
        width: tableWidth[5],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.version" defaultMessage="版本" />
        ),
        width: tableWidth[7],
        dataIndex: 'version',
        key: 'version',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.description" defaultMessage="描述" />
        ),
        width: tableWidth[8],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.createTime"
            defaultMessage="创建时间"
          />
        ),
        width: tableWidth[9],
        dataIndex: 'createTime',
        key: 'createTime',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.isUse"
            defaultMessage="是否使用"
          />
        ),
        width: 80,
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.production"
            defaultMessage="是否投产"
          />
        ),
        width: 80,
        dataIndex: 'running',
        key: 'running',
        render: this.renderComponent(this.renderBoolean)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.fullPull"
            defaultMessage="能否拉全量"
          />
        ),
        width: 100,
        dataIndex: 'ifFullpull',
        key: 'ifFullpull',
        render: this.renderComponent(this.renderIfFullpull)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: tableWidth[12],
        render: this.renderComponent(this.renderAdminOperating)
      }
    ]
    this.userColumns = [
      {
        title: 'DsType',
        width: tableWidth[2],
        dataIndex: 'dsType',
        key: 'dsType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'DsName',
        width: tableWidth[3],
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Schema',
        width: tableWidth[4],
        dataIndex: 'schemaName',
        key: 'schemaName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'TableName',
        width: tableWidth[5],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.isUse"
            defaultMessage="是否使用"
          />
        ),
        width: tableWidth[6],
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: (
          <FormattedMessage id="app.common.version" defaultMessage="版本" />
        ),
        width: tableWidth[7],
        dataIndex: 'version',
        key: 'version',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.description" defaultMessage="描述" />
        ),
        width: tableWidth[8],
        dataIndex: 'description',
        key: 'description',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.createTime"
            defaultMessage="创建时间"
          />
        ),
        width: tableWidth[9],
        dataIndex: 'createTime',
        key: 'createTime',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectResource.table.fullPull"
            defaultMessage="拉全量"
          />
        ),
        width: tableWidth[10],
        dataIndex: 'ifFullpull',
        key: 'ifFullpull',
        render: this.renderComponent(this.renderIfFullpull)
      }
    ]
  }
  componentWillMount () {
    // 初始化查询
    // this.handleSearch(this.initParams, true)
  }

  handleViewEncode = (record) => {
    const {
      onViewEncode
    } = this.props
    onViewEncode(record)
  }

  handleModify = (id) => {
    const {
      onModify
    } = this.props
    onModify(id)
  };

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;

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

  renderIfFullpull =(text, record, index) => {
    let color
    switch (text) {
      case 1:
        color = 'green'
        break
      default:
        color = '#929292'
    }
    return (<div title={text ? 'Y' : 'N'} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text ? 'Y' : 'N'}
      </Tag>
    </div>)
  }

  renderMask = (text, record, index) => {
    let color
    switch (text) {
      case 'request':
        color = 'red'
        break
      case 'none':
        color = 'green'
        break
      default:
        color = '#929292'
    }
    return (<div title={text === 'request' ? 'Y': 'N'} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text === 'request' ? 'Y': 'N'}
      </Tag>
    </div>)
  }

  renderStatus =(text, record, index) => {
    const isY = text === 'use' || text === 'ok'
    let color
    switch (isY) {
      case true:
        color = 'green'
        break
      default:
        color = '#929292'
    }
    return (<div title={isY ? 'Y' : 'N'} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {isY ? 'Y' : 'N'}
      </Tag>
    </div>)
  }

  /**
   * @description true,false
   */
  renderBoolean = (text, record, index) => {
    let color
    switch (text) {
      case true:
        color = 'green'
        break
      default:
        color = '#929292'
    }
    return (<div title={text ? 'Y': 'N'} className={styles.ellipsis}>
      <Tag color={color} style={{cursor: 'auto'}}>
        {text ? 'Y': 'N'}
      </Tag>
    </div>)
  }

  renderUserEncodeOperating = (text, record, index) => (
    text === 'none' ? ('无') : (
    <div>
      <OperatingButton onClick={() => this.handleViewEncode(record)}>
        {'查看'}
      </OperatingButton>
    </div>)
  )

  /**
   * @description selectTable的 option render
   */
  renderAdminOperating = (text, record, index) => (
    <div>
      <OperatingButton icon="edit" onClick={() => this.handleModify(record.projectId)}>
        <FormattedMessage
          id="app.components.projectManage.projectResource.table.changeProjectResourceConfig"
          defaultMessage="修改项目资源配置"
        />
      </OperatingButton>
    </div>
  );
  render () {
    const {
      resourceList,
      onPagination,
      onShowSizeChange,
      projectId
    } = this.props
    console.info('resourceList',resourceList)
    const dataSource = resourceList.result && resourceList.result.payload && resourceList.result.payload.list
    const { loading, loaded } = resourceList
    const { total, pageSize, pageNum } = resourceList.result.payload || {}
    // ID 所属项目 是否投产 数据源类型 数据源名称 Schema TableName Status Version Description CreateTime 拉全量  脱敏要求 操作
    const columns = projectId ? this.userColumns : this.adminColumns
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
          rowKey={record => `${record.tableId}_${record.projectId}`}
          dataSource={dataSource}
          columns={columns}
          pagination={pagination}
          loading={loading}
        />
      </div>
    )
  }
}

ProjectResourceGrid.propTypes = {
  tableWidth: PropTypes.array,
  locale: PropTypes.any,
  resourceList: PropTypes.object,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func
}
