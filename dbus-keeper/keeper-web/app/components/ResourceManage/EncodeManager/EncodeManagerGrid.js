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

  renderEncodeSource = (text, record, index) => {
    let title
    switch (text) {
      case 0:
        title = '源端脱敏'
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
            defaultMessage="项目拓扑表名"
          />
        ),
        dataIndex: 'project_topo_table_id',
        key: 'project_topo_table_id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.basic.name"
            defaultMessage="项目名称"
          />
        ),
        dataIndex: 'project_name',
        key: 'project_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectTable.topoName"
            defaultMessage="拓扑名称"
          />
        ),
        dataIndex: 'topo_name',
        key: 'topo_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="数据源名称"
          />
        ),
        dataIndex: 'ds_name',
        key: 'ds_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSchemaName"
            defaultMessage="Schema名称"
          />
        ),
        dataIndex: 'schema_name',
        key: 'schema_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataTableName"
            defaultMessage="表名"
          />
        ),
        dataIndex: 'table_name',
        key: 'table_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.field_name"
            defaultMessage="列名"
          />
        ),
        dataIndex: 'field_name',
        key: 'field_name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_type"
            defaultMessage="脱敏类型"
          />
        ),
        dataIndex: 'encode_type',
        key: 'encode_type',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_param"
            defaultMessage="脱敏参数"
          />
        ),
        dataIndex: 'encode_param',
        key: 'encode_param',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodeManager.encode_source"
            defaultMessage="脱敏源"
          />
        ),
        dataIndex: 'encode_source',
        key: 'encode_source',
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
        dataIndex: 'encode_plugin_id',
        key: 'encode_plugin_id',
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
