/**
 * @author 戎晓伟
 * @description 路由
 */
import HOCFactory from '@/app/utils/HOCFactory'
// 导入自定义组件
import App from '@/app/containers/App'
import MonitorManageWrapper from '@/app/containers/MonitorManage/MonitorManageWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/monitor-manage',
    component: HOCFactory({'siderHidden': true})(App),
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/monitor-manage/list')
      }
    },
    childRoutes: [
      {
        path: '/monitor-manage/list',
        component: MonitorManageWrapper
      }
    ]
  }
]

