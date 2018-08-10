import React, {PropTypes, Component} from 'react'
import {Row, Col, Modal, Form, Select, Input, Spin, Table, Icon, message} from 'antd'
import {FormattedMessage} from 'react-intl'
import OperatingButton from '@/app/components/common/OperatingButton'

// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class AddLogSchemaTableModal extends Component {
  constructor(props) {
    super(props)
  }

  handleOk = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const {onOk} = this.props
        onOk(values)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {visible, key, defaultOutputTopic, onClose} = this.props
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
      <Modal
        visible={visible}
        maskClosable={false}
        key={key}
        onCancel={onClose}
        onOk={this.handleOk}
        width={1000}
        title={'添加Log table'}
      >
        <Form>
          <FormItem label={'TableName'} {...formItemLayout}>
            {getFieldDecorator('tableName', {
              initialValue: null,
              rules: [
                {
                  required: true,
                  message: 'TableName不能为空',
                  whitespace: true
                }
              ]
            })(<Input
              type="text"
            />)}
          </FormItem>
          <FormItem label={'OutputTopic'} {...formItemLayout}>
            {getFieldDecorator('outputTopic', {
              initialValue: defaultOutputTopic,
              rules: [
                {
                  required: true,
                  message: 'OutputTopic不能为空',
                  whitespace: true
                }
              ]
            })(<Input
              type="text"
            />)}
          </FormItem>
        </Form>
      </Modal>
    )
  }
}

AddLogSchemaTableModal.propTypes = {}
