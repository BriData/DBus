import React, {PropTypes, Component} from 'react'
import {Button, message, Modal, Form, Select, Input, Popconfirm} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item

@Form.create()
export default class DataTableBatchFullPullModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSubmit = () => {
    const {onBatchFullPull} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onBatchFullPull(values)
      }
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, onClose} = this.props
    const formItemLayout = {
      labelCol: {
        xs: {span: 5},
        sm: {span: 6}
      },
      wrapperCol: {
        xs: {span: 19},
        sm: {span: 12}
      }
    }
    return (
      <div className={styles.table}>
        <Modal
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.projectManage.projectTable.fullpull"
            defaultMessage="拉全量"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form>
            <FormItem label="Topic" {...formItemLayout}>
              {getFieldDecorator('topic', {
                initialValue: '',
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  }
                ]
              })(<Input size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableBatchFullPullModal.propTypes = {}
