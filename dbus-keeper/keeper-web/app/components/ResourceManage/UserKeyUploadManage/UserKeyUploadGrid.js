import React, { PropTypes, Component } from 'react'
import { Popconfirm, Form, Select, Input, message, Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option

export default class UserKeyUploadGrid extends Component {
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

  render () {

    const {
      keyList,
      onPagination,
      onShowSizeChange
    } = this.props
    const { total, pageSize, pageNum, list } = keyList
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
        title: <FormattedMessage
          id="app.components.projectManage.encodePlugin.projectId"
          defaultMessage="项目ID"
        />,
        dataIndex: 'id',
        key: 'id',
        width: '8%',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectHome.tabs.basic.name"
          defaultMessage="项目名称"
        />,
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectHome.tabs.basic.owner"
          defaultMessage="项目负责人"
        />,
        dataIndex: 'projectOwner',
        key: 'projectOwner',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.projectHome.tabs.basic.principal"
          defaultMessage="Principal"
        />,
        dataIndex: 'principal',
        key: 'principal',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.components.projectManage.uploadKey.keyPath"
          defaultMessage="密钥路径"
        />,
        dataIndex: 'keytabPath',
        key: 'keytabPath',
        width: '40%',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage
          id="app.common.updateTime"
          defaultMessage="更新时间"
        />,
        dataIndex: 'updateTime',
        key: 'updateTime',
        render: this.renderComponent(this.renderNomal)
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

UserKeyUploadGrid.propTypes = {}
