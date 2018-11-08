/**
 * @author 戎晓伟
 * @description  登录Container组件
 */

import React, { PropTypes, Component } from 'react'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import {message} from 'antd'
import Helmet from 'react-helmet'
// API
import { LOGIN_API } from './api'
// 导入样式
import styles from './res/styles/login.less'
// 导入自定义组件
import { LoginForm,LoginCanvas } from '@/app/components'
// selectors
import {LoginModel} from './selectors'
// action
import { loginList, setParams } from './redux'
import {CHECK_INIT_API} from "@/app/containers/ConfigManage/api";
import Request from "@/app/utils/request";
// 链接reducer和action
@connect(
  createStructuredSelector({
    loginData: LoginModel()
  }),
  dispatch => ({
    getLoginList: params => dispatch(loginList.request(params)),
    setParams: param => dispatch(setParams(param))
  })
)

export default class Login extends Component {
  componentWillMount() {
    const {getLoginList, setParams} = this.props
    if (localStorage.getItem('TOKEN')) {
      // this.props.router.push('/app')
    }
    // 模拟本地存储
    setParams('1223232323')

    // 检查ZK是否有 /DBus 节点，如果没有，则跳转到init页面
    Request(CHECK_INIT_API, {
    })
      .then(res => {
        if (res && res.status === 0) {
          if (!res.payload) {
            message.error('Keeper未初始化')
            this.props.router.push('/init')
          }
        } else {
          message.error('Zookeeper无法访问')
          // this.props.router.push('/init')
        }
      })
      .catch(error => {
        message.error('Zookeeper无法访问')
        // this.props.router.push('/init')
      })
  }
  /**
   * @description 判断浏览器内容
   */
  handleNavigator=(agent) => {
    if (window) {
      const userAgent = window.navigator.userAgent
      return userAgent.indexOf(agent) > -1
    }
    return false
  }
  render () {
    const {getLoginList} = this.props
    return (
      <div>
        <Helmet
          title="Login"
          meta={[{ name: 'description', content: 'Description of Login' }]}
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
            !this.handleNavigator('Firefox') && <LoginCanvas />
          }
          <LoginForm
            router={this.props.router}
            loginApi={LOGIN_API}
            onGetList={getLoginList}
          />
          <div id="loginBg" />
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
