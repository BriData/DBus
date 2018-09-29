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
export default class DataSchemaManageModifyModal extends Component {
  constructor (props) {
    super(props)
    this.state = {
    }
  }

  handleSubmit = () => {
    const {updateApi} = this.props
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        Request(updateApi, {
          data: {
            ...values,
            updateTime: undefined
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              onClose()
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            error.response && error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const {key, visible, schemaInfo, onClose} = this.props
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
          className="top-modal"
          key={key}
          visible={visible}
          maskClosable={false}
          width={1000}
          title={<FormattedMessage
            id="app.components.resourceManage.dataSchema.modifySchema"
            defaultMessage="修改Schema"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form>
            <FormItem label="ID" {...formItemLayout}>
              {getFieldDecorator('id', {
                initialValue: schemaInfo.id,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSchema.dsId"
              defaultMessage="数据源ID"
            />} {...formItemLayout}>
              {getFieldDecorator('dsId', {
                initialValue: schemaInfo.ds_id,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSourceName"
              defaultMessage="数据源名称"
            />} {...formItemLayout}>
              {getFieldDecorator('dsName', {
                initialValue: schemaInfo.ds_name,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSchemaName"
              defaultMessage="Schema名称"
            />} {...formItemLayout}>
              {getFieldDecorator('schemaName', {
                initialValue: schemaInfo.schema_name,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
            <FormItem
              label={<FormattedMessage
                id="app.common.status"
                defaultMessage="状态"
              />} {...formItemLayout}
            >
              {getFieldDecorator('status', {
                initialValue: schemaInfo.status
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select status"
                >
                  <Option value="active" key="active">active</Option>
                  <Option value="inactive" key="inactive">inactive</Option>
                </Select>
              )}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.common.description"
              defaultMessage="描述"
            />} {...formItemLayout}>
              {getFieldDecorator('description', {
                initialValue: schemaInfo.description,
              })(<TextArea autosize={true}/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.common.updateTime"
              defaultMessage="更新时间"
            />} {...formItemLayout}>
              {getFieldDecorator('updateTime', {
                initialValue: schemaInfo.create_time,
              })(<Input disabled={true} size="large" type="text" />)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

DataSchemaManageModifyModal.propTypes = {
}
