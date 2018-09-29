/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col, Modal, Table, message} from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class ProjectTableStartModal extends Component {

  constructor (props) {
    super(props)
    this.tableWidth = [
      '15%',
      '15%',
      '20%',
      '15%',
      '20%',
      '15%'
    ]
  }

  renderComponent = render => (text, record, index) =>
    render(text, record, index)

  /**
   * @description 默认的render
   */
  renderNomal = (text, record, index) => (
    <div title={text} className={styles.ellipsis}>
      {text}
    </div>
  )

  renderOffset = (text, record, index) => {
    const {getFieldDecorator} = this.props.form
    return (
      <div className={styles.ellipsis}>
        {getFieldDecorator(`offset-${record.partition}`, {
          initialValue: (record && record.offset) || '',
          rules: [
            {
              pattern: /^(head|latest|\d*)$/,
              message: 'offset只能为head、latest、数值或空'
            }
          ]
        })(<Input placeholder="Offset" size="large" type="text"/>)}
      </div>
    )
  }

  renderHead = (text, record, index) => {
    const object = {
      [`offset-${record.partition}`] : record.headOffset
    }
    return (
      <div className={styles.ellipsis}>
        <a href="javascript:void(0)" onClick={() => this.props.form.setFieldsValue(object)}>{record.headOffset}</a>
      </div>
    )
  }

  renderLatest = (text, record, index) => {
    const object = {
      [`offset-${record.partition}`] : record.latestOffset
    }
    return (
      <div className={styles.ellipsis}>
        <a href="javascript:void(0)" onClick={() => this.props.form.setFieldsValue(object)}>{record.latestOffset}</a>
      </div>
    )
  }

  handleOk = () => {
    const {onOk} = this.props
    const {partitionList} = this.props
    const partition = partitionList.result ? Object.values(partitionList.result) : []
    this.props.form.validateFields((err, values) => {
      if (!err) {
        onOk(partition, values)
      } else {
        const errorList = Object.values(err)
        errorList.forEach(item => message.error(item.errors[0].message))
      }
    })
  }

  render () {
    const { visible, modalKey} = this.props
    const { onCancel } = this.props
    const { partitionList, affectTableList } = this.props
    const partition = partitionList.result ? Object.values(partitionList.result) : []
    const affectTable = affectTableList.result ? Object.values(affectTableList.result) : []
    const columns = [
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.inputTopic" defaultMessage="输入Topic" />,
        width: this.tableWidth[0],
        dataIndex: 'topic',
        key: 'topic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.partition" defaultMessage="分区" />,
        width: this.tableWidth[1],
        dataIndex: 'partition',
        key: 'partition',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.common.offset" defaultMessage="偏移量" />,
        width: this.tableWidth[2],
        dataIndex: 'latesdOffset',
        key: 'offset',
        render: this.renderComponent(this.renderOffset)
      },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.headOffset" defaultMessage="头部偏移量" />,
        width: this.tableWidth[3],
        key: 'headOffset',
        render: this.renderComponent(this.renderHead)
      },
      // {
      //   title: 'Latest Consumer Offset',
      //   width: this.tableWidth[4],
      //   dataIndex: 'latesdOffset',
      //   key: 'latesdOffset',
      //   render: this.renderComponent(this.renderNomal)
      // },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.latestOffset" defaultMessage="尾部偏移量" />,
        width: this.tableWidth[4],
        key: 'latestOffset',
        render: this.renderComponent(this.renderLatest)
      }
    ]

    return (
      <Modal
        key={modalKey}
        title={<FormattedMessage
          id="app.components.resourceManage.dataTable.start"
          defaultMessage="启动"
        />}
        width={1000}
        visible = {visible}
        onCancel={() => onCancel(false)}
        onOk={this.handleOk}
        maskClosable={false}
      >
        <Form>
          <Table
            size="small"
            rowKey="partition"
            dataSource={partition}
            columns={columns}
          />
        </Form>
        <h3 className={styles.titleNomal} >
          <FormattedMessage
            id="app.components.projectManage.projectTable.affectTable"
            defaultMessage="受影响的表"
          />
        </h3>
        <Row>
          {affectTable.map(affect => (<Col key={affect} span={4}>{affect}</Col>))}
        </Row>
      </Modal>
    )
  }
}

ProjectTableStartModal.propTypes = {
}
