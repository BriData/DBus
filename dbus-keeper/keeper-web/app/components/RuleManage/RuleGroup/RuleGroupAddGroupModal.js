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

@Form.create()
export default class RuleGroupAddGroupModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  handleSubmit = () => {
    const {tableId, addApi, onClose, onRequest} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onRequest({
          api: addApi,
          params: {
            ...values,
            tableId,
            newStatus:'inactive'
          },
          method:'get',
          callback: onClose
        })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, tableInfo, onClose} = this.props
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
          title={'添加规则组'}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}>
            <FormItem label="新规则组名称" {...formItemLayout}>
              {getFieldDecorator('newName', {
                initialValue: null,
                rules: [
                  {
                    required: true,
                    message: '规则组名不能为空'
                  },
                  {
                    pattern: /^\S+$/,
                    message: '请输入正确名称'
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

RuleGroupAddGroupModal.propTypes = {}
