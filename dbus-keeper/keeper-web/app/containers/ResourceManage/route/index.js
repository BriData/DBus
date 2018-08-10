/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import DataSourceWrapper from '@/app/containers/ResourceManage/DataSourceWrapper'
import DataSchemaWrapper from '@/app/containers/ResourceManage/DataSchemaWrapper'
import DataTableWrapper from '@/app/containers/ResourceManage/DataTableWrapper'
import DBusDataWrapper from '@/app/containers/ResourceManage/DBusDataWrapper'
import JarManageWrapper from '@/app/containers/ResourceManage/JarManageWrapper'
import RuleGroupWrapper from '@/app/containers/ResourceManage/RuleGroupWrapper'
import RuleWrapper from '@/app/containers/ResourceManage/RuleWrapper'
import DataSourceCreateWrapper from '@/app/containers/ResourceManage/DataSourceCreateWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/resource-manage',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/resource-manage/data-source')
      }
    },
    childRoutes: [
      {
        path: '/resource-manage/data-source',
        component: DataSourceWrapper
      },
      {
        path: '/resource-manage/data-schema',
        component: DataSchemaWrapper
      },
      {
        path: '/resource-manage/data-table',
        component: DataTableWrapper
      },
      {
        path: '/resource-manage/dbus-data',
        component: DBusDataWrapper
      },
      {
        path: '/resource-manage/jar-manager',
        component: JarManageWrapper
      },
      {
        path: '/resource-manage/rule-group',
        component: RuleGroupWrapper
      },
      {
        path: '/resource-manage/rule',
        component: RuleWrapper
      },
      {
        path: '/resource-manage/datasource-create',
        component: DataSourceCreateWrapper
      },
    ]
  }
]

