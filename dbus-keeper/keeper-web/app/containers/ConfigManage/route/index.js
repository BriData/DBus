/**
 * @author xiancangao
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import ZKManageWrapper from '@/app/containers/ConfigManage/ZKManageWrapper'
import GlobalConfigWrapper from '@/app/containers/ConfigManage/GlobalConfigWrapper'
import HeartbeatConfigWrapper from '@/app/containers/ConfigManage/HeartbeatConfigWrapper'
import DBusMgrConfigWrapper from '@/app/containers/ConfigManage/DBusMgrConfigWrapper'
import DBAEncodeConfigWrapper from '@/app/containers/ConfigManage/DBAEncodeConfigWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/config-manage',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/config-manage/zk-manage')
      }
    },
    childRoutes: [
      {
        path: '/config-manage/zk-manage',
        component: ZKManageWrapper
      },
      {
        path: '/config-manage/global-config',
        component: GlobalConfigWrapper
      },
      {
        path: '/config-manage/heartbeat-config',
        component: HeartbeatConfigWrapper
      },
      {
        path: '/config-manage/dbus-mgr-config',
        component: DBusMgrConfigWrapper
      },
      {
        path: '/config-manage/dba-encode-config',
        component: DBAEncodeConfigWrapper
      },
    ]
  }
]

