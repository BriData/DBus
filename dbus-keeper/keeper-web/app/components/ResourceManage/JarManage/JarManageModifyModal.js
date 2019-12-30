import React, {Component} from 'react'
import {Form, Input, Modal} from 'antd'
import {FormattedMessage} from 'react-intl'
// 导入样式
import styles from './res/styles/index.less'

const FormItem = Form.Item

@Form.create()
export default class JarManageModifyModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSubmit = () => {
    const {onUpdate} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      onUpdate(values)
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, jarInfo, onClose} = this.props
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
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.jarManager.modifyJar"
            defaultMessage="编辑Jar包"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }} className="data-source-modify-form">
            <FormItem label="ID" {...formItemLayout}>
              {getFieldDecorator('id', {
                initialValue: jarInfo.id,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.category"
              defaultMessage="Category"
            />} {...formItemLayout}>
              {getFieldDecorator('category', {
                initialValue: jarInfo.category,
              })(<Input disabled={true} size="large" type="category"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.version"
              defaultMessage="版本"
            />} {...formItemLayout}>
              {getFieldDecorator('version', {
                initialValue: jarInfo.version,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.type"
              defaultMessage="Type"
            />} {...formItemLayout}>
              {getFieldDecorator('type', {
                initialValue: jarInfo.type,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.name"
              defaultMessage="Jar包名称"
            />} {...formItemLayout}>
              {getFieldDecorator('name', {
                initialValue: jarInfo.name,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.minorVersion"
              defaultMessage="小版本"
            />} {...formItemLayout}>
              {getFieldDecorator('minorVersion', {
                initialValue: jarInfo.minorVersion,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.jarManager.path"
              defaultMessage="jar包路径"
            />} {...formItemLayout}>
              {getFieldDecorator('path', {
                initialValue: jarInfo.path,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.common.description"
              defaultMessage="描述"
            />} {...formItemLayout}>
              {getFieldDecorator('description', {
                initialValue: jarInfo.description,
              })(<Input size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

JarManageModifyModal.propTypes = {}
