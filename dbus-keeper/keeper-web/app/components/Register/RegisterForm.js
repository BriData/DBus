/**
 * @author 戎晓伟
 * @description  登录From 组件
 */

import React, { PropTypes, Component } from 'react'
import Request, { setToken } from '@/app/utils/request'
import { Form, Input, Button, message, Row, Col } from 'antd'
import { fromJS, is } from 'immutable'
import md5 from 'js-md5'

// 导入样式
import styles from './res/styles/register.less'

const FormItem = Form.Item

@Form.create()
export default class RegisterForm extends Component {
  /**
   * 注册
   */
  handleSubmit = () => {
    const { registerApi } = this.props
    // {email,userName,password,phoneNum}
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        const param = fromJS(values).delete('repassword')
        Request(registerApi, {
          params: {
            encoded: true
          },
          data: {
            ...param.toJS(),
            password: md5(param.get('password'))
          },
          method: 'post'
        })
          .then(res => {
            if (res && res.status === 0) {
              const loginParam = res.payload && {
                email: res.payload.email,
                password: res.payload.password
              }
              // 登录
              this.handleLogin(loginParam)
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
  };
  /**
   * 登录
   */
  handleLogin = params => {
    const { loginApi } = this.props
    Request(loginApi, {
      params: {
        encoded: true
      },
      data: params,
      method: 'post' })
      .then(res => {
        if (res && res.status === 0) {
          if (res.payload) {
            let TOKEN = res.payload.token
            let username = res.payload.userName
            window.localStorage.setItem('TOKEN', TOKEN)
            setToken(TOKEN)
            window.localStorage.setItem('USERNAME', username)
            window.location.href = '/project-manage'
          }
        } else {
          message.warn(res.message)
        }
      })
      .catch(error => {
        error.response.data && error.response.data.message
          ? message.error(error.response.data.message)
          : message.error(error.message)
      })
  };
  checkPassword = (rule, value, callback) => {
    const form = this.props.form
    if (value && value !== form.getFieldValue('password')) {
      callback('您两次输入的密码不一致')
    } else {
      callback()
    }
  };
  render () {
    const { getFieldDecorator } = this.props.form
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
      <div>
        <Form autoComplete="off"
          className={styles.register}
          onKeyUp={e => {
            e.keyCode === 13 && this.handleSubmit()
          }}
        >
          <FormItem label="用户名" {...formItemLayout}>
            {getFieldDecorator('userName', {
              rules: [
                {
                  required: true,
                  message: '用户名不能为空'
                },
                {
                  pattern: /\S+/,
                  message: '请输入正确用户名'
                }
              ]
            })(<Input placeholder="请输入用户名" size="large" type="text" />)}
          </FormItem>
          <FormItem label="邮箱" {...formItemLayout}>
            {getFieldDecorator('email', {
              rules: [
                {
                  required: true,
                  message: '邮箱不能为空'
                },
                {
                  pattern: /\w[-\w.+]*@([A-Za-z0-9][-A-Za-z0-9]+\.)+[A-Za-z]{2,14}/,
                  message: '请输入正确的邮箱'
                }
              ]
            })(<Input placeholder="请输入邮箱" size="large" type="text" />)}
          </FormItem>
          <FormItem label="密码" {...formItemLayout}>
            {getFieldDecorator('password', {
              rules: [
                {
                  required: true,
                  message: '密码不能为空'
                },
                {
                  pattern: /^(?![0-9]+$)(?![a-zA-Z]+$)(?![!@#$%^&*.]+$)[0-9A-Za-z!@#$%^&*.]{6,16}$/,
                  message: '密码强度较弱，请重新输入'
                }
              ]
            })(
              <Input
                type="password"
                size="large"
                placeholder="6-16位字符（字母、数字、特殊符号），区分大小写"
              />
            )}
          </FormItem>
          <FormItem label="确认密码" {...formItemLayout}>
            {getFieldDecorator('repassword', {
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
                placeholder="请再次确认密码"
              />
            )}
          </FormItem>
          <FormItem label="手机号" {...formItemLayout}>
            {getFieldDecorator('phoneNum', {
              rules: [
                {
                  pattern: /^1[0-9]{10}$/,
                  message: '请输入正确的手机号'
                }
              ]
            })(<Input type="text" placeholder="请输入手机号" size="large" />)}
          </FormItem>
          {/* <FormItem label="手机验证码" {...formItemLayout}>
            <Row gutter={10}>
              <Col span={16}>
                {getFieldDecorator('captcha', {
                  rules: [
                    {
                      required: true,
                      message: 'Please input the captcha you got!'
                    }
                  ]
                })(<Input size="large" placeholder="验证码" />)}
              </Col>
              <Col span={6}>
                <Button size="large" className={styles.captcha}>获取验证码</Button>
              </Col>
            </Row>
          </FormItem> */}
          <FormItem
            wrapperCol={{
              sm: {
                span: 12,
                offset: 6
              }
            }}
          >
            <Button
              type="primary"
              size="large"
              onClick={this.handleSubmit}
              className={styles.submit}
            >
              注册
            </Button>
          </FormItem>
        </Form>
      </div>
    )
  }
}

RegisterForm.propTypes = {
  form: PropTypes.any,
  router: PropTypes.any,
  loginApi: PropTypes.string,
  registerApi: PropTypes.string
}
