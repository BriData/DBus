import React, { PropTypes, Component } from 'react'
import { Modal, Form, Select, Input } from 'antd'
import { FormattedMessage } from 'react-intl'

// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ProjectTableInitialLoadModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  handleSubmit = () => {
    const {initialLoadApi, onClose, record, onRequest} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onRequest({
          api: initialLoadApi,
          params: {
            ...values,
            projectTableId: record.tableId
          },
          callback: onClose
        })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, record, onClose} = this.props
    const formItemLayout = {
      labelCol: {
        xs: { span: 5 },
        sm: { span: 6 }
      },
      wrapperCol: {
        xs: { span: 19 },
        sm: { span: 12 }
      }
    }
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={true}
          width={1000}
          title={'拉全量'}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}>
            <FormItem label="输出topic" {...formItemLayout}>
              {getFieldDecorator('outputTopic', {
                initialValue: record.outputTopic,
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  },
                  {
                    pattern: /^\S+$/,
                    message: '请输入正确topic'
                  }
                ]
              })(<Input size="large" type="text" />)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

ProjectTableInitialLoadModal.propTypes = {
}
