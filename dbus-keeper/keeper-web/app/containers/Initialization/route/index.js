/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import Initialization from '@/app/containers/Initialization'
// 导出路由
export default (store) => [
  {
    path: '/init',
    component: Initialization
  }
]

