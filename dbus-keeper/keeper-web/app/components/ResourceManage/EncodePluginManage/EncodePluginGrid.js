import React, { PropTypes, Component } from 'react'
import { Tag, Tooltip,Popover, Popconfirm, Form, Select, Input, message, Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class EncodePluginGrid extends Component {
  constructor (props) {
    super(props)
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
  /**
   * @description option render
   */
  renderOperating = (text, record, index) => {
    const { onDelete } = this.props
    return (
      <div>
        <Popconfirm title={'确认删除？'} onConfirm={() => onDelete(record)} okText="Yes" cancelText="No">
          <OperatingButton icon="delete">
            <FormattedMessage id="app.common.delete" defaultMessage="删除" />
          </OperatingButton>
        </Popconfirm>

      </div>
    )
  }

  renderStatus = (text, record, index) => {
    let color
    switch (text) {
      case 'active':
        color = 'green'
        break
      case 'inactive':
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

  render () {

    const {
      encodePlugins,
      onPagination,
      onShowSizeChange
    } = this.props
    const { loading, loaded } = encodePlugins
    const { total, pageSize, pageNum, list } = encodePlugins.result.payload || {}
    const encodePluginList = encodePlugins.result.payload && encodePlugins.result.payload.list
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


    const columns = [
      {
        title: 'ID',
        dataIndex: 'id',
        width: '8%',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.name"
          defaultMessage="名称"
        />,
        dataIndex: 'name',
        key: 'name',
        width: '12%',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodePlugin.projectId"
          defaultMessage="项目ID"
        />,
        dataIndex: 'projectId',
        width: '12%',
        key: 'projectId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectHome.tabs.basic.name"
          defaultMessage="项目名称"
        />,
        dataIndex: 'projectDisplayName',
        width: '20%',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodePlugin.path"
          defaultMessage="插件路径"
        />,
        dataIndex: 'path',
        width: '25%',
        key: 'path',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.encodePlugin.encoders"
          defaultMessage="脱敏方法"
        />,
        dataIndex: 'encoders',
        width: '25%',
        key: 'encoders',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.status"
          defaultMessage="状态"
        />,
        dataIndex: 'status',
        width: 80,
        key: 'status',
        render: this.renderComponent(this.renderStatus)
      },
      {
        title: <FormattedMessage
          id="app.common.updateTime"
          defaultMessage="更新时间"
        />,
        dataIndex: 'updateTime',
        width: 150,
        key: 'updateTime',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        width: 100,
        key: 'operate',
        render: this.renderComponent(this.renderOperating)
      }
    ]
    return (
      <div className={styles.table}>
        <Table
          rowKey={record => record.id}
          dataSource={encodePluginList}
          columns={columns}
          pagination={pagination}
        />
      </div>
    )
  }
}

EncodePluginGrid.propTypes = {}
