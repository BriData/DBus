/**
 * @author xiancangao
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import ControlMessageWrapper from '@/app/containers/toolSet/ControlMessageWrapper'
import GlobalFullpullWrapper from '@/app/containers/toolSet/GlobalFullpullWrapper'
import KafkaReaderWrapper from '@/app/containers/toolSet/KafkaReaderWrapper'
import BatchRestartTopo from '@/app/containers/toolSet/BatchRestartTopoWrapper'
import OggCanalDeployWrapper from '@/app/containers/toolSet/OggCanalDeployWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/tool-set',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/tool-set/kafka-reader')
      }
    },
    childRoutes: [
      {
        path: '/tool-set/control-message',
        component: ControlMessageWrapper
      },
      {
        path: '/tool-set/global-fullpull',
        component: GlobalFullpullWrapper
      },
      {
        path: '/tool-set/kafka-reader',
        component: KafkaReaderWrapper
      },
      {
        path: '/tool-set/batch-restart-topo',
        component: BatchRestartTopo
      },
      {
        path: '/tool-set/deploy-info',
        component: OggCanalDeployWrapper
      }
    ]
  }
]

