/**
 * @author 戎晓伟
 * @description  登录From 组件
 */

import React, { PropTypes, Component } from 'react'
import Request, { setToken } from '@/app/utils/request'
import { Icon, Form, Input, Button, message } from 'antd'
import md5 from 'js-md5'

// 导入样式
import styles from './res/styles/login.less'

const FormItem = Form.Item

@Form.create()
export default class LoginForm extends Component {
  /**
   * 登录
   */
  doLogin = () => {
    const {loginApi} = this.props
    // this.props.onGetList({a: 1111})
    this.props.form.validateFieldsAndScroll((err, values) => {
      if (!err) {
        let param = values
        Request(loginApi, {
          params: {
            encoded: true
          },
          data: {
            ...param,
            password: md5(param.password)
          },
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
          .catch(error => message.error(error))
      }
    })
  };
  render () {
    const { getFieldDecorator } = this.props.form
    return (
      <div className={styles.loginPanel}>
        <h2 className={styles.loginTitle}>DBus</h2>
        <Form
          className="form-login"
          onKeyUp={e => {
            e.keyCode === 13 && this.doLogin()
          }}
        >
          <FormItem>
            {getFieldDecorator('email', {
              rules: [
                {
                  required: true,
                  message: '帐号不能为空'
                }
              ]
            })(
              <Input
                prefix={<Icon type="user" style={{ color: '#fff' }} />}
                className={styles.antInput}
                placeholder="请输入邮箱"
                type="text"
              />
            )}
          </FormItem>
          <FormItem>
            {getFieldDecorator('password', {
              rules: [
                {
                  required: true,
                  message: '密码不能为空'
                }
              ]
            })(
              <Input
                prefix={<Icon type="lock" style={{ color: '#fff' }} />}
                className={styles.antInput}
                name="password"
                type="password"
                placeholder="请输入密码"
              />
            )}
          </FormItem>
        </Form>
        <Button type="primary" onClick={this.doLogin} className={styles.antBtn}>
          登录
        </Button>
        <br/>
        <a href="/register">注册</a>
        {/*<a style={{float: 'right'}} href="/init">初始化</a>*/}
      </div>
    )
  }
}

LoginForm.propTypes = {
  form: PropTypes.any,
  router: PropTypes.any,
  loginApi: PropTypes.string,
  onGetList: PropTypes.func
}
