import React, { Component } from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import Helmet from 'react-helmet'
import { createStructuredSelector } from 'reselect'
import { getUserInfo } from '@/app/utils/request'

import { Navigator, Header, Foot } from '@/app/components'

import {
  makeSelectNavCollapsed,
  makeSelectNavSource,
  makeSelectTopNavSource
} from './selectors'
import {
  openNavigator,
  closeNavigator,
  loadNavSource,
  loadTopNavSource
} from './actions'

import topMenu from '@/app/assets/json/topMenu.json'

import { Layout, Icon } from 'antd'
// 切换语言
import { changeLocale } from '../LanguageProvider/actions'
import { makeSelectLocale } from '../LanguageProvider/selectors'

import styles from './App.less'

const { Content, Footer, Sider } = Layout

export class App extends Component {
  componentWillMount () {
    // 判断登录是否失效
    localStorage && this.handleTurnLogin()
    // 初始化菜单
    this.userInfo && this.handleInitMenus(this.userInfo.roleType)
  }
  /**
   * 判断登录是否失效
   */
  handleTurnLogin = () => {
    let TOKEN = localStorage.getItem('TOKEN')
    this.USERNAME = localStorage.getItem('USERNAME')
    // 用户信息
    this.userInfo = getUserInfo()
    if (!TOKEN) {
      // token 不存在跳转回登录
      this.props.router.push('/login')
      window.location.href = '/login'
    }
  };
  /**
   * @param rotype [object String] 用户权限
   * @description 根据用户权限初始化菜单
  */
  handleInitMenus = rotype => {
    if (rotype === 'admin') {
      this.leftMenu = require('@/app/assets/json/leftMenu/admin.json')
      this.handleGetMenus('top', 'admin', () => {
        const {router} = this.props
        const pathName = router.routes[1].path.substr(1)
        this.handleGetMenus('left', pathName)
      })
    } else {
      this.leftMenu = require('@/app/assets/json/leftMenu/user.json')
      this.handleGetMenus('top', 'user', () => {
        const { router } = this.props
        const pathName = router.routes[1].path.substr(1)
        this.handleGetMenus('left', pathName)
      })
    }
  };
  /**
   * 退出登录
   */
  handleLoginOut = () => {
    localStorage.removeItem('TOKEN')
    localStorage.removeItem('USERNAME')
    window.location.href = '/login'
    // this.props.router.push('/login')
  };
  /**
   * @description 二级菜单click
   */
  navClick = item => {
    const { router, location } = this.props
    const ProjectSearch = (router.routes[1].path.substr(1) === 'project') && location.query
    const key = item.name
    const href = item.href
    const selectedKey =
      router &&
      router.routes[2] &&
      router.routes[2].path.substr(1).split('/')[1]
    if (key !== selectedKey) {
      if (href) {
        window.location.href = href
      } else {
        if (ProjectSearch) {
          router.push({pathname: `${router.routes[1].path}/${key}`, query: {...ProjectSearch}})
        } else {
          router.push(`${router.routes[1].path}/${key}`)
        }
        // window.location.href = `${router.routes[1].path}/${key}`
      }
    }
  };
  /**
   * @description 侧边菜单收缩与展开
   */
  handleCollapse = collapsed => {
    const { navCollapsed, onOpenNavigator, onCloseNavigator } = this.props
    if (navCollapsed) {
      onOpenNavigator()
    } else {
      onCloseNavigator()
    }
  };
  /**
   * @param name [object String] JSON name {'left'|'top'}
   * @param key  [object String] 菜单key
   * @param callback [object Function] 回调方法
   * @description 从JSON文件中输出菜单
   */
  handleGetMenus = (name, key, callback) => {
    let menus = []
    if (name === 'top') {
      menus = topMenu[key]
      this.props.onChangeTopNavSource(menus)
    } else if (name === 'left') {
      menus = this.leftMenu[key]
      this.props.onChangeNavSource(menus)
    }
    if (Object.prototype.toString.call(callback) === '[object Function]') {
      callback()
    }
  };

  render () {
    const {
      navCollapsed,
      navSource,
      topNavSource,
      siderHidden,
      children,
      router,
      onOpenNavigator,
      onCloseNavigator,
      onChangeLocale,
      onChangeNavSource,
      locale
    } = this.props

    const selectedNav =
      router &&
      router.routes[2] &&
      router.routes[2].path.substr(1) &&
      router.routes[2].path.substr(1).split('/')[1]
    return (
      <div className={styles.app}>
        <Helmet
          titleTemplate="%s - DBus Keeper"
          defaultTitle="DBus Keeper"
          meta={[
            {
              name: 'description',
              content: 'DBus Keeper Manage System'
            }
          ]}
        />
        <Layout className={styles.layout}>
          <Header
            userName={this.USERNAME}
            userInfo={this.userInfo}
            navCollapsed={navCollapsed}
            locale={locale}
            topMenu={topNavSource}
            siderHidden={siderHidden || !navSource}
            router={router}
            onOpenNavigator={onOpenNavigator}
            onCloseNavigator={onCloseNavigator}
            onChangeLocale={onChangeLocale}
            onChangeNavSource={onChangeNavSource}
            onGetMenus={this.handleGetMenus}
            onLoginOut={this.handleLoginOut}
            className={styles.header}
            />
          <Layout className={styles.main}>
            {!siderHidden &&
            !!navSource && (
            <Sider
              collapsible
              className={styles.slider}
              collapsed={navCollapsed}
              onCollapse={this.handleCollapse}
              width={130}
              collapsedWidth={50}
              trigger={
                <Icon type={navCollapsed ? 'menu-unfold' : 'menu-fold'} />
                }
              >
              <Navigator
                menu={navSource}
                collapsed={navCollapsed}
                selected={selectedNav}
                onNavClick={this.navClick}
                className={styles.navigator}
                />
            </Sider>
            )}
            <Content className={styles.content}>
              {React.Children.toArray(children)}
            </Content>
          </Layout>
        </Layout>
      </div>
    )
  }
}

App.propTypes = {
  location: PropTypes.any,
  navCollapsed: PropTypes.bool,
  navSource: PropTypes.array,
  topNavSource: PropTypes.array,
  locale: PropTypes.any,
  children: PropTypes.node,
  router: PropTypes.any,
  onOpenNavigator: PropTypes.func,
  onCloseNavigator: PropTypes.func,
  onChangeLocale: PropTypes.func,
  onChangeNavSource: PropTypes.func,
  onChangeTopNavSource: PropTypes.func,
  siderHidden: PropTypes.any
}

const mapStateToProps = createStructuredSelector({
  navCollapsed: makeSelectNavCollapsed(),
  navSource: makeSelectNavSource(),
  locale: makeSelectLocale(),
  topNavSource: makeSelectTopNavSource()
})

function mapDispatchToProps (dispatch) {
  return {
    onOpenNavigator: () => dispatch(openNavigator()),
    onCloseNavigator: () => dispatch(closeNavigator()),
    onChangeLocale: local => dispatch(changeLocale(local)),
    onChangeNavSource: params => dispatch(loadNavSource(params)),
    onChangeTopNavSource: params => dispatch(loadTopNavSource(params))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(App)
