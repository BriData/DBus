/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import Register from '@/app/containers/Register'
// 导出路由
export default (store) => [
  {
    path: '/register',
    component: Register
  }
]

