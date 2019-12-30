/**
 * @author xiancangao
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import ClusterCheckWrapper from '@/app/containers/SelfCheck/ClusterCheckWrapper'
import CanalCheckWrapper from "@/app/containers/SelfCheck/CanalCheckWrapper";
import OggCheckWrapper from "@/app/containers/SelfCheck/OggCheckWrapper";
// 导出路由
export default (store) => [
  {
    path: '/self-check',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/self-check/cluster-check')
      }
    },
    childRoutes: [
      {
        path: '/self-check/cluster-check',
        component: ClusterCheckWrapper
      },
      {
        path: '/self-check/canal-status',
        component: CanalCheckWrapper
      },
      {
        path: '/self-check/ogg-status',
        component: OggCheckWrapper
      }
    ]
  }
]

