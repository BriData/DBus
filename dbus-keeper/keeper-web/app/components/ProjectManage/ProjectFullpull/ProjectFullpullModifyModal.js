import React, {Component} from 'react'
import {Form, Input, message, Modal, Select} from 'antd'
import {FormattedMessage} from 'react-intl'
import {FULLPULL_HISTORY_UPDATE_API} from '@/app/containers/ProjectManage/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from "@/app/utils/request";

const FormItem = Form.Item
const Option = Select.Option
const TextArea = Input.TextArea

@Form.create()
export default class ProjectFullpullModifyModal extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  handleSubmit = () => {
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      Request(FULLPULL_HISTORY_UPDATE_API, {
        data: values,
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
          error.response.data && error.response.data.message
            ? message.error(error.response.data.message)
            : message.error(error.message)
        })
    })
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const {key, visible, fullpullInfo, onClose} = this.props
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
            id="app.common.modify"
            defaultMessage="修改"
          />}
          onCancel={onClose}
          onOk={this.handleSubmit}
        >
          <Form autoComplete="off" onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }} className="data-source-modify-form">
            <FormItem label="ID" {...formItemLayout}>
              {getFieldDecorator('id', {
                initialValue: fullpullInfo.id,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.pullType"
              defaultMessage="拉取方式"
            />} {...formItemLayout}>
              {getFieldDecorator('type', {
                initialValue: fullpullInfo.type,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.project"
              defaultMessage="项目名称"
            />} {...formItemLayout}>
              {getFieldDecorator('projectDisplayName', {
                initialValue: fullpullInfo.projectDisplayName,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSourceName"
              defaultMessage="数据源名称"
            />} {...formItemLayout}>
              {getFieldDecorator('dsName', {
                initialValue: fullpullInfo.dsName,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataSchemaName"
              defaultMessage="Schema名称"
            />} {...formItemLayout}>
              {getFieldDecorator('schemaName', {
                initialValue: fullpullInfo.schemaName,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.resourceManage.dataTableName"
              defaultMessage="表名"
            />} {...formItemLayout}>
              {getFieldDecorator('tableName', {
                initialValue: fullpullInfo.tableName,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.firstSplitOffset"
              defaultMessage="首片Offset"
            />} {...formItemLayout}>
              {getFieldDecorator('firstShardMsgOffset', {
                initialValue: fullpullInfo.firstShardMsgOffset,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.lastSplitOffset"
              defaultMessage="末片Offset"
            />} {...formItemLayout}>
              {getFieldDecorator('lastSplitOffset', {
                initialValue: fullpullInfo.lastShardMsgOffset,
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.currentShardOffset"
              defaultMessage="当前片Offset"
            />} {...formItemLayout}>
              {getFieldDecorator('currentShardOffset', {
                initialValue: fullpullInfo.currentShardOffset,
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem
              label={<FormattedMessage
                id="app.components.projectManage.projectFullpullHistory.table.status"
                defaultMessage="状态"
              />} {...formItemLayout}
            >
              {getFieldDecorator('state', {
                initialValue: `${fullpullInfo.state}`
              })(
                <Select
                  showSearch
                  optionFilterProp='children'
                  className={styles.select}
                  placeholder="Select state"
                >
                  <Option value="init" key="init">init</Option>
                  <Option value="splitting" key="splitting">splitting</Option>
                  <Option value="pulling" key="pulling">pulling</Option>
                  <Option value="ending" key="ending">ending</Option>
                  <Option value="abort" key="abort">abort</Option>
                </Select>
              )}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.projectManage.projectFullpullHistory.table.errMsg"
              defaultMessage="出错信息"
            />} {...formItemLayout}>
              {getFieldDecorator('errorMsg', {
                initialValue: fullpullInfo.errorMsg,
              })(<Input size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

ProjectFullpullModifyModal.propTypes = {}
