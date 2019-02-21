import React, { PropTypes, Component } from 'react'
import { Tooltip, Form, Switch ,Popconfirm, Select, Input, message,Table ,Tag } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import dateFormat from 'dateformat'
const FormItem = Form.Item
const Option = Select.Option


export default class DBAEncodeConfigGrid extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '5%',
    ]
  }

  /**
   * @param render 传入一个render
   * @returns render 返回一个新的render
   * @description 统一处理render函数
   */
  renderComponent = render => (text, record, index) =>
    render(text, record, index);

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <Tooltip title={text}>
      <div className={styles.ellipsis}>
        {text}
      </div>
    </Tooltip>
  )

  renderOverride = (text, record, index) => {
    const {onDbaEncodeUpdate} = this.props
    return (
      <Switch
        checked={!!text}
        onChange={value => onDbaEncodeUpdate({
          id: record.id,
          override: Number(value)
        })}
      />
    )
  }

  render () {
    const {
      dbaEncodeList,
      onPagination,
      onShowSizeChange,
    } = this.props
    const { total, pageSize, pageNum } = dbaEncodeList.result
    const dbaEncodes = dbaEncodeList.result && dbaEncodeList.result.list
    const columns = [
      {
        title: 'ID',
        width: this.tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceName"
            defaultMessage="数据源名称"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'dsName',
        key: 'dsName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSourceType"
            defaultMessage="数据源类型"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'dsType',
        key: 'dsType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.resourceManage.dataSchemaName"
            defaultMessage="Schema名称"
          />
        ),
        width: this.tableWidth[0],
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
        width: this.tableWidth[0],
        dataIndex: 'tableName',
        key: 'tableName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.fieldName"
            defaultMessage="列名"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'fieldName',
        key: 'fieldName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.pluginId"
            defaultMessage="脱敏插件ID"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'pluginId',
        key: 'pluginId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.encodeType"
            defaultMessage="脱敏方法"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'encodeType',
        key: 'encodeType',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.encodeParam"
            defaultMessage="脱敏参数"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'encodeParam',
        key: 'encodeParam',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.truncate"
            defaultMessage="是否截断"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'truncate',
        key: 'truncate',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.configCenter.dbaEncodeConfig.override"
            defaultMessage="是否截断"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'override',
        key: 'override',
        render: this.renderComponent(this.renderOverride)
      },
      {
        title: (
          <FormattedMessage
            id="app.common.description"
            defaultMessage="描述"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'desc_',
        key: 'desc_',
        render: this.renderComponent(this.renderNomal)
      },
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
          rowKey={record => record.id}
          dataSource={dbaEncodes}
          columns={columns}
          pagination={pagination}
        />
      </div>
    )
  }
}

DBAEncodeConfigGrid.propTypes = {
}
