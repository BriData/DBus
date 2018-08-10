/**
 * @author 戎晓伟
 * @description 路由
 */
import HOCFactory from '@/app/utils/HOCFactory'
// 导入自定义组件
import App from '@/app/containers/App'
import UserManageWrapper from '@/app/containers/UserManage/UserManageWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/user-manage',
    component: HOCFactory({'siderHidden': true})(App),
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/user-manage/list')
      }
    },
    childRoutes: [
      {
        path: '/user-manage/list',
        component: UserManageWrapper
      }
    ]
  }
]

