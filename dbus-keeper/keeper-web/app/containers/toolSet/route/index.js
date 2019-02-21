/**
 * @author xiancangao
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import ControlMessageWrapper from '@/app/containers/ToolSet/ControlMessageWrapper'
import GlobalFullpullWrapper from '@/app/containers/ToolSet/GlobalFullpullWrapper'
import KafkaReaderWrapper from '@/app/containers/ToolSet/KafkaReaderWrapper'
import BatchRestartTopo from '@/app/containers/ToolSet/BatchRestartTopoWrapper'
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
      }
    ]
  }
]

