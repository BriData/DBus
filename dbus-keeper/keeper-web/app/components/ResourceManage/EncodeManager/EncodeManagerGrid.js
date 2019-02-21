/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Tooltip, Table, message,Tag } from 'antd'
import { FormattedMessage } from 'react-intl'
import Request from '@/app/utils/request'
import OperatingButton from '@/app/components/common/OperatingButton'
// API
import {

} from '@/app/containers/ProjectManage/api'

// 导入样式
import styles from './res/styles/index.less'

export default class EncodeManagerGrid extends Component {

  /**
   * @param key 传入一个key type:[Object String]  默认:空
   * @returns 返回一个随机字符串
   */
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`;
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

  renderSpecialApprove = (text, record, index) => {
    let title
    switch (text) {
      case 1:
        title = 'Y'
        break
      default:
        title = 'N'
    }
    return (<div title={title} className={styles.ellipsis}>
      {title}
    </div>)
  }

  renderEncodeSource = (text, record, index) => {
    let title
    switch (text) {
      case 0:
        title = 'DBA脱敏'
        break
      case 1:
        title = '项目级脱敏'
        break
      case 2:
        title = '自定义脱敏'
        break
      case 3:
        title = '无'
        break
      default:
        title = '脱敏类型未识别'
    }
    return (<div title={title} className={styles.ellipsis}>
      {title}
    </div>)
  }

  render () {
    const {
      encodeList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading } = encodeList
    const { total, pageSize, pageNum, list } = encodeList.result
    const dataSource = list || []
    const columns = [
      {
        title: (
          'ID'
        ),
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.project_topo_table_id"
            defaultMessage="项目拓扑表ID"
          />
        ),
        dataIndex: 'tpttId',
        key: 'tpttId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.basic.name"
            defaultMessage="项目名称"
          />
        ),
        dataIndex: 'projectName',
        key: 'projectName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.topoName"
            defaultMessage="拓扑名称"
          />
        ),
        dataIndex: 'topoName',
        key: 'topoName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="数据源名称"
          />
        ),
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSchemaName"
            defaultMessage="Schema名称"
          />
        ),
        dataIndex: 'schemaName',
        key: 'schemaName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataTableName"
            defaultMessage="表名"
          />
        ),
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.field_name"
            defaultMessage="列名"
          />
        ),
        dataIndex: 'fieldName',
        key: 'fieldName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.resource.specialApprove"
            defaultMessage="特批不脱敏"
          />
        ),
        dataIndex: 'specialApprove',
        key: 'specialApprove',
        render: this.renderComponent(this.renderSpecialApprove)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_type"
            defaultMessage="脱敏类型"
          />
        ),
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_param"
            defaultMessage="脱敏参数"
          />
        ),
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_source"
            defaultMessage="脱敏源"
          />
        ),
        dataIndex: 'encodeSource',
        key: 'encodeSource',
        render: this.renderComponent(this.renderEncodeSource)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_plugin_id"
            defaultMessage="脱敏插件ID"
          />
        ),
        width: '12%',
        dataIndex: 'encodePluginId',
        key: 'encodePluginId',
        render: this.renderComponent(this.renderNomal)
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

EncodeManagerGrid.propTypes = {
  locale: PropTypes.any,
  onPagination: PropTypes.func,
  onShowSizeChange: PropTypes.func,
  onSearch: PropTypes.func,
}
