import React from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import { FormattedMessage } from 'react-intl'
import Menu from 'antd/lib/menu'
import Icon from 'antd/lib/icon'
const MenuItem = Menu.Item

import styles from './Navigator.less'

export function Navigator (props) {
  const menuItems =
    props.menu &&
    props.menu.map(m => (
      <MenuItem key={m.name}>
        <Icon type={m.icon} />
        <span><FormattedMessage id={m.id} defaultMessage={m.text} /></span>
      </MenuItem>
    ))
  const navigatorClass = classnames({
    [props.className]: true,
    [styles.navigator]: true,
    [styles.collapsed]: props.collapsed
  })

  return (
    <div className={navigatorClass}>
      <Menu
        mode="inline"
        theme="dark"
        selectedKeys={[props.selected]}
        inlineCollapsed={props.collapsed}
        onClick={navClick(props)}
        className="DBus-menu"
      >
        {menuItems}
      </Menu>
    </div>
  )
}

const navClick = props => ({ item, key }) => {
  // 过滤到当前菜单
  const currentMenu = props.menu.filter(item => item.name === key)[0]
  props.onNavClick(currentMenu)
}

Navigator.propTypes = {
  menu: PropTypes.array,
  selected: PropTypes.string,
  collapsed: PropTypes.bool,
  className: PropTypes.string,
  onNavClick: PropTypes.func // eslint-disable-line
}

Navigator.defaultProps = {
  collapsed: true
}

export default Navigator
