/**
 * @author 戎晓伟
 * @description  注册Container组件
 */

import React, { PropTypes, Component } from 'react'
import Helmet from 'react-helmet'
// API
import {REGISTER_API} from './api'
import {LOGIN_API} from '../Login/api'
// 导入样式
import styles from './res/styles/register.less'
// 导入自定义组件
import { RegisterForm, Foot } from '@/app/components'

export default class Login extends Component {
  componentWillMount () {}
  render () {
    const {router} = this.props
    return (
      <div>
        <Helmet
          title="Register"
          meta={[{ name: 'description', content: 'Description of Register' }]}
        />
        <div className={styles.registerContainer}>
          <div className={styles.header}>
            <div className={styles.layout}>
              <h1 className={styles.title}>欢迎注册</h1>
              <span className={styles.right}>已有账号？<a href="/login">请登录</a></span>
            </div>
          </div>
          <div className={styles.layout} style={{marginTop: '5%'}}>
            <div className={styles.layoutLeft}>
              <RegisterForm
                router={router}
                loginApi={LOGIN_API}
                registerApi={REGISTER_API}
              />
            </div>
            <div className={styles.layoutRight}>
              <ul>
                <li><p>注意事项：</p></li>
                <li><p>密码须为6-16位字符（字母、数字、特殊符号），区分大小写</p></li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    )
  }
}
Login.propTypes = {
  router: PropTypes.any
}
