/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import Login from '@/app/containers/Login'
// 导出路由
export default (store) => [
  {
    path: '/login',
    component: Login
  }
]

