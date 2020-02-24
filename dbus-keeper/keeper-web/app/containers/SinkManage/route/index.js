/**
 * @author 戎晓伟
 * @description 路由
 */
// 导入自定义组件
import App from '@/app/containers/App'
import SinkHomeWrapper from '@/app/containers/SinkManage/SinkHomeWrapper'
import SinkerTopologyWrapper from '@/app/containers/SinkManage/SinkerTopologyWrapper'
import SinkerSchemaWrapper from '@/app/containers/SinkManage/SinkerSchemaWrapper'
import SinkerTableWrapper from '@/app/containers/SinkManage/SinkerTableWrapper'
// 导出路由
export default (store) => [
  {
    path: '/sink-manage',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/sink-manage/sink-manage')
      }
    },
    childRoutes: [
      {
        path: '/sink-manage/sink-manage',
        component: SinkHomeWrapper
      },
      {
        path: '/sink-manage/sinker-manage',
        component: SinkerTopologyWrapper
      },
      {
        path: '/sink-manage/schema-manage',
        component: SinkerSchemaWrapper
      },
      {
        path: '/sink-manage/table-manage',
        component: SinkerTableWrapper
      }
    ]
  }
]

