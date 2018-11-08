import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input, Button, message,Table } from 'antd'
import { FormattedMessage } from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea


export default class DataSourceManageMountModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
    this.tableWidth = [
      '33%',
      '33%',
      '33%'
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
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  render () {
    const {key, visible, content, onClose} = this.props
    const columns = [
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.encodePlugin.projectId"
            defaultMessage="项目ID"
          />
        ),
        width: this.tableWidth[0],
        dataIndex: 'id',
        key: 'id',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.basic.name"
            defaultMessage="项目名称"
          />
        ),
        width: this.tableWidth[1],
        dataIndex: 'projectDisplayName',
        key: 'projectDisplayName',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: (
          <FormattedMessage
            id="app.components.projectManage.projectHome.tabs.basic.owner"
            defaultMessage="项目负责人"
          />
        ),
        width: this.tableWidth[2],
        dataIndex: 'projectOwner',
        key: 'projectOwner',
        render: this.renderComponent(this.renderNomal)
      }
    ]
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataSource.mountedProject"
            defaultMessage="已挂载项目"
          />}
          onCancel={onClose}
          footer={[<Button type="primary" onClick={onClose}>
            <FormattedMessage
              id="app.common.back"
              defaultMessage="返回"
            />
          </Button>]}
        >
          <Table
            rowKey={record => record.id}
            dataSource={content}
            columns={columns}
            pagination={false}
          />
        </Modal>
      </div>
    )
  }
}

DataSourceManageMountModal.propTypes = {
}
