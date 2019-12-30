import React, {PropTypes, Component} from 'react'
import {getUserInfo} from '@/app/utils/request'

export default class Permission extends Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  componentWillMount() {
    const {type, redirect, code} = this.props
    this.flag = true
    switch (type) {
      case 'menu':
        // 处理menu函数
        this.flag = filterRouter(code)
        break
      case 'router':
        // 处理router函数
        break
      default:
        // 处理其他元素函数
        break
    }
  }

  /**
   * @description 判断一级菜单是否存在
   */
  handleFilterMenus = (menus, code) => {
    const topMenuArray = menus.topMenu.map(item => item.name)
    return topMenuArray.filter(item => item === code)
  };

  handleFilterRouter = menus => {
  };

  render() {
    const {children} = this.props
    return <span>{children}</span>
  }
}

export function getMenus(rotype) {
  if (rotype === 'admin') {
    return {
      topMenu: require('@/app/assets/json/topMenu.json').admin,
      leftMenu: require('@/app/assets/json/leftMenu/admin.json')
    }
  } else {
    return {
      topMenu: require('@/app/assets/json/topMenu.json').user,
      leftMenu: require('@/app/assets/json/leftMenu/user.json')
    }
  }
}

export function filterRouter(code) {
  if (!getUserInfo()) {
    return false
  }
  const userInfo = getUserInfo()
  const Menus = getMenus(userInfo.roleType)
  const topMenuArray = Menus.topMenu.map(item => item.name)
  return topMenuArray.some(item => item === code)
}

// 根据权限过滤路由
export function routerPermission(routes) {
  let temporary = []
  routes.slice(0, routes.length - 1).forEach(item => {
    const code = item.path.substring(1)
    if (code === 'login' || code === 'register' || code === 'init' || code === 'handle') {
      temporary.push(item)
    } else if (code === 'project-manage') {
      if (!filterRouter(code)) {
        const manageRoute = item
        manageRoute['childRoutes'] = manageRoute['childRoutes'].slice(0, 1)
        temporary.push(manageRoute)
      } else {
        temporary.push(item)
      }
    } else {
      if (filterRouter(code)) {
        temporary.push(item)
      }
    }
  })
  temporary.push(routes[routes.length - 1])
  return temporary
}

Permission.propTypes = {
  children: PropTypes.node,
  type: PropTypes.any,
  redirect: PropTypes.any
}
