/**
 * @author 戎晓伟
 * @description  基本信息设置
 */

import React, { PropTypes, Component } from 'react'
import { Modal, Form, Input, Row, Col, Button, Select, message } from 'antd'
import Request from '@/app/utils/request'
import { FormattedMessage } from 'react-intl'
import { intlMessage } from '@/app/i18n'
import md5 from 'js-md5'
// 导入API
import {
  CREATE_USER_API,
  UPDATE_USER_API
} from '@/app/containers/UserManage/api'
// 导入样式
import styles from './res/styles/index.less'
const FormItem = Form.Item
const Option = Select.Option
@Form.create({ warppedComponentRef: true })
export default class UserForm extends Component {
  constructor (props) {
    super(props)
    this.state = {
      reset: false,
      loading: false
    }
    this.formMessage = {
      en: {
        descriptionPlaceholder: 'description,Up to 150 words',
        projectNameMessage: 'The project name is required',
        principalMessage: 'The principal is required',
        topologyMessage:
          'The number of topology must be integers and 0<topologyNum<=100.'
      },
      zh: {
        descriptionPlaceholder: '项目描述，最多150字符',
        projectNameMessage: '项目名称为必填项',
        principalMessage: '负责人为必填项',
        topologyMessage: 'topology 个数必须为整数且 0<topologyNum<=100'
      }
    }
    this.dataTypeGroup = [
      { value: 'user', text: 'User' },
      { value: 'app', text: 'App' },
      { value: 'admin', text: 'Admin' }
    ]
    this.formItemLayout = {
      labelCol: { span: 4 },
      wrapperCol: { span: 12 }
    }
    this.initPassword = '12345678'
  }
  /**
   * @deprecated 提交数据
   */
  handleSubmit = () => {
    const {
      modalStatus,
      userInfo,
      onCloseModal,
      onSearch,
      userListParams
    } = this.props
    const { result } = userInfo
    const requestAPI =
      modalStatus === 'create' ? CREATE_USER_API : UPDATE_USER_API
    const { reset } = this.state
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let param =
          modalStatus === 'modify'
            ? {
              ...result,
              ...values,
              password: reset ? md5('12345678') : result.password
            }
            : { ...values, password: md5('12345678') }
        this.setState({ loading: true })
        Request(requestAPI, {
          params: {
            encoded: true
          },
          data: {
            ...param
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              onCloseModal(false)
              onSearch(userListParams, false)
            } else {
              message.warn(res.message)
            }
            this.setState({ loading: false })
          })
          .catch(error => {
            error.response.data && error.response.data.message
              ? message.error(error.response.data.message)
              : message.error(error.message)
            this.setState({ loading: false })
          })
      }
    })
  };
  /**
   * @deprecated input placeholder
   */
  handlePlaceholder = fun => id =>
    fun({
      id: 'app.components.input.placeholder',
      valus: {
        name: fun({ id })
      }
    });
  handleResetPassword = (e) => {
    e.preventDefault()
    message.success('重置成功')
    this.setState({ reset: true })
  };
  render () {
    const { getFieldDecorator } = this.props.form
    const { reset, loading } = this.state
    const {
      visibal,
      onCloseModal,
      userInfo,
      modalStatus,
      modalKey
    } = this.props
    const localeMessage = intlMessage(this.props.locale, this.formMessage)
    const placeholder = this.handlePlaceholder(localeMessage)
    const user = modalStatus === 'modify' ? userInfo.result : {}
    return (
      <Modal
        key={modalKey}
        visible={visibal}
        maskClosable={false}
        width={'800px'}
        style={{ top: 60 }}
        onCancel={() => onCloseModal(false)}
        onOk={this.handleSubmit}
        confirmLoading={loading}
        title={modalStatus === 'modify' ? <FormattedMessage
          id="app.components.userManage.modifyUser"
          defaultMessage="修改用户"
        /> : <FormattedMessage
          id="app.components.userManage.createUser"
          defaultMessage="创建用户"
        />}
      >
        {
          // 姓名 邮箱 用户类型 手机号 登录密码
        }
        <Form autoComplete="off" layout="horizontal">
          <FormItem
            label={
              <FormattedMessage
                id="app.common.user.name"
                defaultMessage="姓名"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('userName', {
              initialValue: (user && user.userName) || '',
              rules: [
                {
                  required: true,
                  message: '请输入正确用户名'
                },
                {
                  pattern: /\S+/,
                  message: '请输入正确用户名'
                }
              ]
            })(
              <Input
                type="text"
                placeholder={placeholder('app.common.user.name')}
                onBlur={this.handleBlur}
              />
            )}
          </FormItem>
          <FormItem
            label={
              <FormattedMessage
                id="app.common.user.email"
                defaultMessage="邮箱"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('email', {
              initialValue: (user && user.email) || '',
              rules: [
                {
                  required: true,
                  message: '请输入正确的邮箱'
                },
                {
                  pattern: /\w[-\w.+]*@([A-Za-z0-9][-A-Za-z0-9]+\.)+[A-Za-z]{2,14}/,
                  message: '请输入正确的邮箱'
                }
              ]
            })(
              <Input
                type="text"
                placeholder={placeholder('app.common.user.email')}
              />
            )}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.userManage.userType"
            defaultMessage="用户类型"
          />} {...this.formItemLayout}>
            {getFieldDecorator('roleType', {
              initialValue: (user && user.roleType) || 'user'
            })(
              <Select
                showSearch
                optionFilterProp='children'
              >
                {this.dataTypeGroup.map(item => (
                  <Option value={item.value} key={item.value}>
                    {item.text}
                  </Option>
                ))}
              </Select>
            )}
          </FormItem>
          <FormItem
            label={
              <FormattedMessage
                id="app.common.user.phone"
                defaultMessage="手机号"
              />
            }
            {...this.formItemLayout}
          >
            {getFieldDecorator('phoneNum', {
              initialValue: (user && user.phoneNum) || '',
              rules: [
                {
                  pattern: /^1[0-9]{10}$/,
                  message: '请输入正确的手机号'
                }
              ]
            })(<Input placeholder={placeholder('app.common.user.phone')} />)}
          </FormItem>
          <FormItem label={<FormattedMessage
            id="app.components.userManage.password"
            defaultMessage="密码"
          />} {...this.formItemLayout}>
            <Row gutter={10}>
              <Col span={modalStatus === 'modify' ? 18 : 24}>
                <Input
                  size="large"
                  value={
                    user && user.password && !reset ? '*******' : '系统默认密码'
                  }
                  disabled
                />
                {reset && (
                  <p style={{color: '#f04134'}}>您重置后密码为：{this.initPassword}，点击确定后生效</p>
                )}
              </Col>
              {modalStatus === 'modify' && (
                <Col span={6}>
                  <Button
                    size="large"
                    className={styles.resetPassword}
                    onClick={this.handleResetPassword}
                  >
                    <FormattedMessage
                      id="app.components.userManage.reset"
                      defaultMessage="重置"
                    />
                  </Button>
                </Col>
              )}
            </Row>
          </FormItem>
        </Form>
      </Modal>
    )
  }
}

UserForm.propTypes = {
  locale: PropTypes.any,
  modalKey: PropTypes.string,
  form: PropTypes.object,
  userInfo: PropTypes.object,
  modalStatus: PropTypes.string,
  visibal: PropTypes.bool,
  onCloseModal: PropTypes.func,
  onSearch: PropTypes.func,
  userListParams: PropTypes.object
}
