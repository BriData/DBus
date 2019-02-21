/**
 * @param children [object Any]     提示内容或者展示内容
 * @param icon          [object String]  Button 图标
 * @param className     [object String]  组件自定义class
 * @param type          [object String]  Button type  primary|dashed|danger|''
 * @param buttonParams   [object Object]  组件其他属性  非必填
 * @description 操作按钮 icon为空时展示文本按钮否则展示图标按钮
 */

import React, { Component, PropTypes } from 'react'
import { Button, Tooltip, Dropdown, Menu, Icon, Popconfirm } from 'antd'
// 导入样式
import styles from './OperatingButton.less'

export default class OperatingButton extends Component {
  /**
   * @param children [object Any]     Button 提示内容
   * @param icon          [object String]  Button 图标
   * @param className     [object String]  Button 自定义class
   * @param type          [object String]  Button type  primary|dashed|danger|''
   * @param buttonParams   [object Object]  组件其他属性  非必填
   * @returns reactNode 返回一个Button组件
   */
  renderCircleButton = props => {
    const {disabled, children, icon, type, className, buttonParams, onClick } = props
    return (
      <Tooltip placement="top" title={children}>
        <Button
          disabled={disabled}
          type={type}
          icon={icon}
          shape="circle"
          className={`${styles.button} ${className || ''}`}
          onClick={onClick}
          {...buttonParams}
        />
      </Tooltip>
    )
  };
  /**
   * @param children [object Any]     展示内容
   * @param className     [object String]  Button 自定义class
   * @param buttonParams   [object Object]  组件其他属性  非必填
   * @returns reactNode 返回一个ButtonText组件
   */
  renderTextButton = props => {
    const { children, className, buttonParams, onClick } = props
    return (
      <a className={`${className || ''}`} onClick={onClick} {...buttonParams}>
        {children}
      </a>
    )
  };
  renderDropdown = props => {
    const { menus, icon } = props
    const menuMap = (
      <Menu>
        {menus.map((item, index) => {
          if (item.isDivider) {
            return <Menu.Divider/>
          } else {
            return (
              <Menu.Item disabled={item.disabled} key={`menu_${index}`}>
                {(
                  item.disabled ? (
                      <span className={styles.menu}>
                        {item.icon && <Icon type={item.icon}/>}
                        {item.text}
                      </span>
                    ) :
                    (item.confirmText ?
                        <Popconfirm title={item.confirmText} onConfirm={item.onClick || null} okText="Yes"
                                    cancelText="No">
                          <a className={styles.menu}>
                            {item.icon && <Icon type={item.icon}/>}
                            {item.text}
                          </a>
                        </Popconfirm>
                        :
                        <a className={styles.menu} onClick={item.onClick || null}>
                          {item.icon && <Icon type={item.icon}/>}
                          {item.text}
                        </a>
                    )
                )}
              </Menu.Item>
            )
          }
        })}
      </Menu>
    )
    return (
      <Dropdown trigger={['click']} overlay={menuMap} placement="bottomLeft">
        <Button icon={icon} shape="circle" className={styles.button} />
      </Dropdown>
    )
  };
  render () {
    const { icon, menus } = this.props
    return (
      <span>
        {icon
          ? menus
            ? this.renderDropdown(this.props)
            : this.renderCircleButton(this.props)
          : this.renderTextButton(this.props)}
      </span>
    )
  }
}

OperatingButton.propTypes = {
  icon: PropTypes.string,
  menus: PropTypes.array,
  children: PropTypes.any,
  onClick: PropTypes.func,
  type: PropTypes.string,
  className: PropTypes.string,
  buttonParams: PropTypes.object
}
