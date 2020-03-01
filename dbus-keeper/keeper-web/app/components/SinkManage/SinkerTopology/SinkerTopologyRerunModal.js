/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, {Component} from 'react'
import {Form, Input, message, Modal, Table} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'

@Form.create()
export default class SinkerTopologyRerunModal extends Component {

  constructor(props) {
    super(props)
    this.tableWidth = [
      '35%',
      '15%',
      '15%',
      '35%'
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
    // 表单中的key不能有点，所以全部替换为特殊标记
    return (
      <div className={styles.ellipsis}>
        {getFieldDecorator(JSON.stringify(record).replace(/\./g, '@@@'), {
          initialValue: null,
          rules: [
            {
              pattern: /^\d*$/,
              message: '请输入正确数字格式',
              whitespace: true
            }
          ]
        })(<Input
          placeholder="Please input number"
          size="small"
          type="text"/>)}
      </div>
    )
  }

  constructForm = (values) => {
    const {record} = this.props
    let arr = []
    let index = 0
    Object.keys(values).map(key => {
      // 表单中的key不能有点，所以全部替换为特殊标记，此处恢复
      const item = JSON.parse(key.replace(/@@@/g, '.'))
      arr[index] = {...item, position: values[key]}
      index++
    })
    const result = {
      sinkerName: record.sinkerName,
      topicInfo: arr
    }
    return result
  }

  handleOk = () => {
    this.props.form.validateFields((err, values) => {
      if (!err) {
        const {topologyRerunApi} = this.props
        Request(topologyRerunApi, {
          data: this.constructForm(values),
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              message.success(res.message)
              const {onClose} = this.props
              onClose()
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
      } else {
        const errorList = Object.values(err)
        errorList.forEach(item => message.error(item.errors[0].message))
      }
    })
  }

  render() {
    const {visible, key, rerunInitResult} = this.props
    const {onClose} = this.props
    const columns = [
      {
        title: 'Topic',
        width: this.tableWidth[0],
        dataIndex: 'topic',
        key: 'topic',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.partition" defaultMessage="分区"/>,
        width: this.tableWidth[1],
        dataIndex: 'partition',
        key: 'partition',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.headOffset" defaultMessage="头部偏移量"/>,
        width: this.tableWidth[2],
        dataIndex: 'beginOffset',
        key: 'beginOffset',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.components.projectManage.projectTable.latestOffset" defaultMessage="尾部偏移量"/>,
        width: this.tableWidth[2],
        dataIndex: 'endOffset',
        key: 'endOffset',
        render: this.renderComponent(this.renderNomal)
      },
      {
        title: <FormattedMessage id="app.common.offset" defaultMessage="偏移量"/>,
        width: this.tableWidth[3],
        key: 'offset',
        render: this.renderComponent(this.renderOffset)
      }
    ]

    return (
      <Modal
        key={key}
        title={'拖回重跑'}
        width={1000}
        visible={visible}
        onCancel={onClose}
        onOk={this.handleOk}
        maskClosable={false}
      >
        <Form>
          <Table
            size="small"
            rowKey={record => `${record.topic}_${record.partition}`}
            dataSource={rerunInitResult}
            columns={columns}
            pagination={false}
          />
        </Form>
      </Modal>
    )
  }
}

SinkerTopologyRerunModal.propTypes = {}
