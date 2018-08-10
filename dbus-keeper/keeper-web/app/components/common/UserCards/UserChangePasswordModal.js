/**
 * @author 戎晓伟
 * @description  Sink信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Form, Select, Input, Button, Row, Col, Modal, Table, message} from 'antd'
import { FormattedMessage } from 'react-intl'
// 导入样式
import Request from "@/app/utils/request";
import {getUserInfo} from "@/app/utils/request";
import md5 from 'js-md5'
import {USER_CHANGE_PASSWORD_API} from '@/app/containers/UserManage/api'
const FormItem = Form.Item
const Option = Select.Option

@Form.create()
export default class ProjectTableStartModal extends Component {

  constructor (props) {
    super(props)
  }

  checkPassword = (rule, value, callback) => {
    const form = this.props.form
    if (value && value !== form.getFieldValue('newPassword')) {
      callback('您两次输入的密码不一致')
    } else {
      callback()
    }
  }

  handleSubmit = () => {
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const userInfo = getUserInfo()
        Request(USER_CHANGE_PASSWORD_API, {
          params: {
            encoded: true,
            oldPassword: md5(values.oldPassword),
            newPassword: md5(values.newPassword),
            id: userInfo.userId
          },
          method: 'get'
        })
          .then(res => {
            if (res && res.status === 0) {
              message.success(res.message)
              this.props.onClose()
            } else {
              message.warn(res.message)
            }
          })
          .catch(error => {
            error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
          })
      }
    })
  }

  render () {
    const { getFieldDecorator } = this.props.form
    const { visible, key, onClose} = this.props
    const formItemLayout = {
      labelCol: { span: 4 },
      wrapperCol: { span: 18 }
    }
    return (
      <Modal
        key={key}
        title={'修改密码'}
        width={800}
        visible = {visible}
        onCancel={onClose}
        onOk={this.handleSubmit}
        maskClosable={false}
      >
        <Form autoComplete="off"
          onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}
        >
          <FormItem label="原始密码" {...formItemLayout}>
            {getFieldDecorator('oldPassword', {
              rules: [
                {
                  required: true,
                  message: '原始密码不能为空'
                }
              ]
            })(
              <Input
                type="password"
                size="large"
                placeholder="原始密码"
              />
            )}
          </FormItem>
          <FormItem label="新密码" {...formItemLayout}>
            {getFieldDecorator('newPassword', {
              rules: [
                {
                  required: true,
                  message: '密码不能为空'
                },
                {
                  pattern: /^(?![0-9]+$)(?![a-zA-Z]+$)(?![!@#$%^&*.]+$)[0-9A-Za-z!@#$%^&*.]{6,16}$/,
                  message: '密码强度较弱，请重新输入(6-16位字母、数字、特殊符号，区分大小写)'
                }
              ]
            })(
              <Input
                type="password"
                size="large"
                placeholder="新密码，6-16位字符（字母、数字、特殊符号），区分大小写"
              />
            )}
          </FormItem>
          <FormItem label="再次确认新密码" {...formItemLayout}>
            {getFieldDecorator('rePassword', {
              rules: [
                {
                  required: true,
                  message: '请再次确认密码'
                },
                {
                  validator: this.checkPassword
                }
              ]
            })(
              <Input
                type="password"
                size="large"
                placeholder="请再次确认新密码"
              />
            )}
          </FormItem>
        </Form>
      </Modal>
    )
  }
}

ProjectTableStartModal.propTypes = {
}
