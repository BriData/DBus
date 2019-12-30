/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import TopologyWrapper from '@/app/containers/project/TopologyWrapper'
import TableWrapper from '@/app/containers/project/TableWrapper'
import SinkWrapper from '@/app/containers/project/SinkWrapper'
import UserWrapper from '@/app/containers/project/UserWrapper'
import ResourceWrapper from '@/app/containers/project/ResourceWrapper'
import FullpullWrapper from '@/app/containers/Project/FullpullWrapper'
import MonitorWrapper from '@/app/containers/Project/MonitorWrapper'
import UserKeyDownloadWrapper from '@/app/containers/Project/UserKeyDownloadWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/project',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/project/table')
      }
    },
    childRoutes: [
      {
        path: '/project/resource',
        component: ResourceWrapper
      },
      {
        path: '/project/topology',
        component: TopologyWrapper
      },
      {
        path: '/project/table',
        component: TableWrapper
      },
      {
        path: '/project/sink',
        component: SinkWrapper
      },
      {
        path: '/project/user',
        component: UserWrapper
      },
      {
        path: '/project/fullpull-history',
        component: FullpullWrapper
      },
      {
        path: '/project/monitor',
        component: MonitorWrapper
      },
      {
        path: '/project/user-key-download-manager',
        component: UserKeyDownloadWrapper
      },

    ]
  }
]

