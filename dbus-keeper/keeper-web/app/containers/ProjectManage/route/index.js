/**
 * @author 戎晓伟
 * @description 路由
 */

// 导入自定义组件
import App from '@/app/containers/App'
import ProjectHomeWrapper from '@/app/containers/ProjectManage/ProjectHomeWrapper'
import ProjectResourceWrapper from '@/app/containers/ProjectManage/ProjectResourceWrapper'
import ProjectTableWrapper from '@/app/containers/ProjectManage/ProjectTableWrapper'
import ProjectTopologyWrapper from '@/app/containers/ProjectManage/ProjectTopologyWrapper'
import ProjectFullpullWrapper from '@/app/containers/ProjectManage/ProjectFullpullWrapper'
import EncodeManageWrapper from '@/app/containers/ProjectManage/EncodeManageWrapper'
import EncodePluginWrapper from '@/app/containers/ProjectManage/EncodePluginWrapper'
import UserKeyUploadWrapper from '@/app/containers/ProjectManage/UserKeyUploadWrapper'
// HOCFactory({'siderHidden': true})(App)
// 导出路由
export default (store) => [
  {
    path: '/project-manage',
    component: App,
    indexRoute: {
      onEnter: (_, replace) => {
        let TOKEN = window.localStorage.getItem('TOKEN')
        if (!TOKEN) {
          replace('/login')
        }
        replace('/project-manage/home')
      }
    },
    childRoutes: [
      {
        path: '/project-manage/home',
        component: ProjectHomeWrapper
      },
      {
        path: '/project-manage/resource',
        component: ProjectResourceWrapper
      },
      {
        path: '/project-manage/topology',
        component: ProjectTopologyWrapper
      },
      {
        path: '/project-manage/table',
        component: ProjectTableWrapper
      },
      {
        path: '/project-manage/fullpull-history',
        component: ProjectFullpullWrapper
      },
      {
        path: '/project-manage/encode-manager',
        component: EncodeManageWrapper
      },
      {
        path: '/project-manage/encode-plugin-manager',
        component: EncodePluginWrapper
      },
      {
        path: '/project-manage/user-key-upload-manager',
        component: UserKeyUploadWrapper
      }
    ]
  }
]

