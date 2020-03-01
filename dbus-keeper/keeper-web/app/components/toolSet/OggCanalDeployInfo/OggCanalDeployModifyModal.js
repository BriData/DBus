import React, {Component} from 'react'
import {Form, Input, message, Modal} from 'antd'
import {FormattedMessage} from 'react-intl'
import {UPDATE_OGG_CANAL_DEPLOY_INFO} from '@/app/containers/toolSet/api'
// 导入样式
import styles from './res/styles/index.less'
import Request from '@/app/utils/request'

const FormItem = Form.Item

@Form.create()
export default class OggCanalDeployModifyModal extends Component {
  constructor (props) {
    super(props)
    this.state = {}
  }

  handleSubmit = () => {
    const {onClose} = this.props
    this.props.form.validateFieldsAndScroll((err, values) => {
      Request(UPDATE_OGG_CANAL_DEPLOY_INFO, {
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

  render () {
    const {getFieldDecorator} = this.props.form
    const {key, visible, deployInfo, onClose} = this.props
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
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.host"
              defaultMessage="host"
            />} {...formItemLayout}>
              {getFieldDecorator('host', {
                initialValue: deployInfo.host
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.canalPath"
              defaultMessage="canal小工具目录"
            />} {...formItemLayout}>
              {getFieldDecorator('canalPath', {
                initialValue: deployInfo.canalPath
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.maxCanalNumber"
              defaultMessage="canal最大部署数"
            />} {...formItemLayout}>
              {getFieldDecorator('maxCanalNumber', {
                initialValue: deployInfo.maxCanalNumber
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.deployCanalNumber"
              defaultMessage="canal实际部署数"
            />} {...formItemLayout}>
              {getFieldDecorator('deployCanalNumber', {
                initialValue: deployInfo.deployCanalNumber
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>

            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.oggPath"
              defaultMessage="ogg根目录"
            />} {...formItemLayout}>
              {getFieldDecorator('oggPath', {
                initialValue: deployInfo.oggPath
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.oggToolPath"
              defaultMessage="ogg小工具目录"
            />} {...formItemLayout}>
              {getFieldDecorator('oggToolPath', {
                initialValue: deployInfo.oggToolPath
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.oggTrailPath"
              defaultMessage="oggTrailPath"
            />} {...formItemLayout}>
              {getFieldDecorator('oggTrailPath', {
                initialValue: deployInfo.oggTrailPath
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.mgrReplicatPort"
              defaultMessage="mgr进程端口号"
            />} {...formItemLayout}>
              {getFieldDecorator('mgrReplicatPort', {
                initialValue: deployInfo.mgrReplicatPort
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.maxReplicatNumber"
              defaultMessage="replicat最大部署数"
            />} {...formItemLayout}>
              {getFieldDecorator('maxReplicatNumber', {
                initialValue: deployInfo.maxReplicatNumber
              })(<Input size="large" type="text"/>)}
            </FormItem>
            <FormItem label={<FormattedMessage
              id="app.components.toolset.CanalOggDeployInfo.deployReplicatNumber"
              defaultMessage="replicat实际部署数"
            />} {...formItemLayout}>
              {getFieldDecorator('deployReplicatNumber', {
                initialValue: deployInfo.deployReplicatNumber
              })(<Input disabled={true} size="large" type="text"/>)}
            </FormItem>
          </Form>
        </Modal>
      </div>
    )
  }
}

OggCanalDeployModifyModal.propTypes = {}
