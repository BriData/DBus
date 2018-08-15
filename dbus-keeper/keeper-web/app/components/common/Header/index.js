import React from 'react'
import PropTypes from 'prop-types'
import { Avatar, Icon } from 'antd'
import { FormattedMessage } from 'react-intl'
import UserCards from '../UserCards'
import styles from './Header.less'
import logo from '../../../logo.png'

export function Header (props) {
  return (
    <div className={`${props.className} ${styles.header}`}>
      <div className={styles.left}>
        <div className={styles.logo}>
          <h2><img style={{verticalAlign: 'middle'}} height={36} width={36} src={logo}/> KEEPER</h2>
        </div>
        <ul>
          {props.topMenu &&
            props.topMenu.map(item => (
                item.name !== 'project' && (
                  <li
                    onClick={() => turnRoute(props, item)}
                    key={item.name}
                    className={
                      iSActive(props.router, item.name) ? styles.active : ''
                    }
                  >
                    <span>
                      {item.icon && <Icon type={item.icon} />}
                      <FormattedMessage
                        id={item.id}
                        defaultMessage={item.text}
                      />
                    </span>
                  </li>
                )
              ))}
        </ul>
      </div>
      <div className={styles.right}>
        <ul>
          <li onClick={() => changeLocale(props)}>
            <span>
              <FormattedMessage id="app.components.navigator.zh.en" />
            </span>
          </li>
          <li>
            <UserCards
              userName={props.userName}
              userInfo={props.userInfo}
              userImg="https://zos.alipayobjects.com/rmsportal/ODTLcjxAfvqbxHnVXCYX.png"
              onLoginOut={props.onLoginOut}
            />
          </li>
        </ul>
      </div>
    </div>
  )
}

// 切换语言
const changeLocale = props => {
  if (props.locale === 'zh') {
    props.onChangeLocale('en')
  } else if (props.locale === 'en') {
    props.onChangeLocale('zh')
  }
}

// 判断是否为选中状态
const iSActive = (router, name) => {
  const path = router && router.routes[1] && router.routes[1].path.substr(1)
  return path === name
}

// 路由跳转
const turnRoute = (props, item) => {
  if (item.href) {
    // 如如默认路径存在 则跳转默认路径
    props.router.push(`${item.href}`)
    props.onGetMenus(null)
    return false
  }
  props.onGetMenus('left', item.name)
  if (!iSActive(props.router, item.name)) {
    // props.router.push(`/${item.name}`)
    window.location.href = `/${item.name}`
  }
}
Header.propTypes = {
  userName: PropTypes.string,
  userInfo: PropTypes.object,
  topMenu: PropTypes.array,
  className: PropTypes.string,
  siderHidden: PropTypes.any,
  onLoginOut: PropTypes.func,
  onOpenNavigator: PropTypes.func, // eslint-disable-line
  onCloseNavigator: PropTypes.func // eslint-disable-line
}

export default Header
