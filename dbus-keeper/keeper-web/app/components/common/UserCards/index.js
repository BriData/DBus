/**
 * @param children [object Any]     提示内容或者展示内容
 * @param icon          [object String]  Button 图标
 * @param className     [object String]  组件自定义class
 * @param type          [object String]  Button type  primary|dashed|danger|''
 * @param buttonParams   [object Object]  组件其他属性  非必填
 * @description 操作按钮 icon为空时展示文本按钮否则展示图标按钮
 */

import React, { Component, PropTypes } from 'react'
import { Button, Tooltip, Row, Col, Avatar } from 'antd'
import UserChangePasswordModal from './UserChangePasswordModal'
// 导入样式
import styles from './UserCards.less'

export default class UserCards extends Component {
  constructor (props) {
    super(props)
    this.state = {
      flag: false,

      passwordModalVisible: false,
      passwordModalKey: 'passwordModalKey',
    }
  }
  handleRandom = key =>
    `${Math.random()
      .toString(32)
      .substr(3, 8)}${key || ''}`

  handleOpenPasswordModal = () => {
    this.setState({
      passwordModalVisible: true,
      passwordModalKey: this.handleRandom('passwordModalKey')
    })
  }

  handleClosePasswordModal = () => {
    this.setState({
      passwordModalVisible: false,
      passwordModalKey: this.handleRandom('passwordModalKey')
    })
  }
  /**
   * @description 隐藏卡片内容
   */
  handleMouseLeave = () => {
    this.setState({ flag: false })
  };

  /**
   * @description 显示卡片内容
   */
  handleMouseEnter = () => {
    this.setState({ flag: true })
  };
  /**
   * @description 权限内容
   */
  handleRoleType=(rotype) => {
    switch (rotype) {
      case 'admin':
        return '管理员'
      default:
        return '普通用户'
    }
  }
  /**
   *@description 用户头像处理
   */
  renderAvatar=(rotype) => {
    switch (rotype) {
      case 'admin':
        return <Avatar className={styles.userName} style={{backgroundColor: '#FFBF00'}}>超</Avatar>
      default:
        return <Avatar className={styles.userName} style={{backgroundColor: '#66CCCC'}}>普</Avatar>
    }
  }

  render () {
    const {flag} = this.state
    const { userName, userImg, onLoginOut, onChangePassword, userInfo } = this.props
    const {passwordModalKey, passwordModalVisible} = this.state
    return (
      <div onMouseLeave={this.handleMouseLeave} className={styles.userOut}>
        <div className={styles.user} onMouseEnter={this.handleMouseEnter}>
          {
            userInfo && this.renderAvatar(userInfo.roleType)
          }
          <span>{userName}</span>
        </div>
        <div className={styles.dropdownMemu} style={{display: `${flag ? 'block' : 'none'}`}}>
          <div className={styles.cardLayout}>
            <div className={styles.body}>
              <div className={styles.line}>
                <span>账户：</span>
                <span>{userInfo && userInfo.email}</span>
              </div>
              <div className={styles.line}>
                <span>权限：</span>
                <span>{userInfo && this.handleRoleType(userInfo.roleType)}</span>
              </div>
            </div>
            <div className={styles.footer}>
              <Row>
                <Col
                  span={12}
                  className={styles.col}
                  onClick={this.handleOpenPasswordModal}
                >
                  <span>修改密码</span>
                </Col>
                <Col span={12} className={styles.col} onClick={onLoginOut}>
                  <span>退出系统</span>
                </Col>
              </Row>
            </div>
          </div>
        </div>
        <UserChangePasswordModal
          key={passwordModalKey}
          visible={passwordModalVisible}
          onClose={this.handleClosePasswordModal}
        />
      </div>
    )
  }
}

UserCards.propTypes = {
  userName: PropTypes.string,
  userInfo: PropTypes.object,
  userImg: PropTypes.string,
  onLoginOut: PropTypes.func,
  onChangePassword: PropTypes.func
}
