/**
 * @author 戎晓伟
 * @description  登录Container组件
 */

import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {createStructuredSelector} from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
// API
import {LOGIN_API} from './api'
// 导入样式
import styles from './res/styles/login.less'
// 导入自定义组件
import {LoginForm, LoginCanvas} from '@/app/components'

import {CHECK_INIT_API} from "@/app/containers/ConfigManage/api";
import Request from "@/app/utils/request";
import {makeSelectLocale} from "@/app/containers/LanguageProvider/selectors";
// 链接reducer和action
@connect(
  createStructuredSelector({
    locale: makeSelectLocale(),
  }),
  dispatch => ({})
)

export default class Login extends Component {
  componentWillMount() {
  }

  /**
   * @description 判断浏览器内容
   */
  handleNavigator = (agent) => {
    if (window) {
      const userAgent = window.navigator.userAgent
      return userAgent.indexOf(agent) > -1
    }
    return false
  }

  render() {
    const {getLoginList} = this.props
    return (
      <div>
        <Helmet
          title="Login"
          meta={[{name: 'description', content: 'Description of Login'}]}
        />
        <div
          id="loginContainer"
          className={styles.loginContainer}
          style={{
            background: `url(${require('./res/images/background.jpg')})`,
            backgroundSize: 'cover',
          }}
        >
          {
            !this.handleNavigator('Firefox') && <LoginCanvas/>
          }
          <LoginForm
            router={this.props.router}
            loginApi={LOGIN_API}
            onGetList={getLoginList}
          />
          <div id="loginBg"/>
        </div>
      </div>
    )
  }
}
Login.propTypes = {
  router: PropTypes.any,
  getLoginList: PropTypes.func,
  setParams: PropTypes.func
}
