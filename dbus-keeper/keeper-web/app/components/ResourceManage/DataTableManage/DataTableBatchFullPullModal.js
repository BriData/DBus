import React, {Component} from 'react'
import {Form, Input, Modal, Radio} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item
const RadioGroup = Radio.Group
@Form.create()
export default class DataTableBatchFullPullModal extends Component {
  constructor(props) {
    super(props)
    this.state = {
      sinkType: 'KAFKA'
    }
  }

  handleSubmit = () => {
    const {onBatchFullPull} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        onBatchFullPull(values)
      }
    })
  }

  onChange = (e) => {
    this.setState({
      sinkType: e.target.value
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, onClose} = this.props
    const {sinkType} = this.state
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
            <FormItem label='全量类型' {...formItemLayout}>
              <RadioGroup {...formItemLayout} onChange={this.onChange} value={sinkType}>
                <Radio value='KAFKA'>KAFKA</Radio>
                <Radio value='HDFS'>HDFS</Radio>
              </RadioGroup>
            </FormItem>
            {sinkType === 'KAFKA' && (<FormItem label="Topic" {...formItemLayout}>
              {getFieldDecorator('topic', {
                initialValue: '',
                rules: [
                  {
                    required: true,
                    message: 'topic不能为空'
                  }
                ]
              })(<Input size="large" type="text" placeholder="mysql1.test1"/>)}
            </FormItem>)}
            {sinkType === 'HDFS' && (<FormItem label="HDFS数据根目录" {...formItemLayout}>
              {getFieldDecorator('hdfsRootPath', {
                rules: [
                  {
                    required: true,
                    message: 'hdfs root path不能为空'
                  }
                ]
              })(<Input size="large" type="text" placeholder="/datahub/dbus"/>)}
            </FormItem>)}
          </Form>
        </Modal>
      </div>
    )
  }
}

DataTableBatchFullPullModal.propTypes = {}
