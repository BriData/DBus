// 添加子路由
import LoginRoute from '@/app/containers/Login/route'
import ProjectManageRoute from '@/app/containers/ProjectManage/route'
import RegisterRoute from '@/app/containers/Register/route'
import ProjectRoute from '@/app/containers/Project/route'
import UserManageRoute from '@/app/containers/UserManage/route'
import SinkManageRoute from '@/app/containers/SinkManage/route'
import MonitorManageRoute from '@/app/containers/MonitorManage/route'
import ResourceManageRoute from '@/app/containers/ResourceManage/route'
import ToolSetRoute from '@/app/containers/toolSet/route'
import SelfCheckRoute from '@/app/containers/SelfCheck/route'
import ConfigManageRoute from '@/app/containers/ConfigManage/route'

const errorLoading = (err) => {
  console.error('Dynamic page loading failed', err)
}

const loadModule = (cb) => (componentModule) => {
  cb(null, componentModule.default)
}

export default function createRoutes(store) {
  return [
    // 登录
    ...LoginRoute(store),
    // 注册
    ...RegisterRoute(store),
    // 项目管理
    ...ProjectManageRoute(store),
    // 子项目管理
    ...ProjectRoute(store),
    // 用户管理
    ...UserManageRoute(store),
    // Sink管理
    ...SinkManageRoute(store),
    // Monitor监控
    ...MonitorManageRoute(store),
    // Resource管理
    ...ResourceManageRoute(store),
    // ToolSet管理
    ...ToolSetRoute(store),
    // 自我检查
    ...SelfCheckRoute(store),
    // 配置中心管理
    ...ConfigManageRoute(store),
    {
      path: '*',
      name: 'notfound',
      getComponent(nextState, cb) {
        import('containers/NotFoundPage')
          .then(loadModule(cb))
          .catch(errorLoading)
      }
    }
  ]
}
