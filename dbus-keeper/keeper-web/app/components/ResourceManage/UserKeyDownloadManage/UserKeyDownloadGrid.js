import React, { PropTypes, Component } from 'react'
import { Popconfirm, Form, Select, Input, message, Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class UserKeyDownloadGrid extends Component {
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
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  );
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
        title: (
          <FormattedMessage id="app.common.operate" defaultMessage="操作" />
        ),
        key: 'operate',
        render: this.renderComponent(this.renderOperating)
      },
      {
        title: 'ID',
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Name',
        dataIndex: 'name',
        key: 'name',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'ProjectId',
        dataIndex: 'projectId',
        key: 'projectId',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'ProjectName',
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Path',
        dataIndex: 'path',
        key: 'path',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Encoders',
        dataIndex: 'encoders',
        key: 'encoders',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: 'UpdateTime',
        dataIndex: 'updateTime',
        key: 'updateTime',
        render: this.renderComponent(this.renderNomal)
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

UserKeyDownloadGrid.propTypes = {}
