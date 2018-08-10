import appSagas from './containers/App/sagas'
// 导入saga
import LoginSaga from '@/app/containers/Login/sagas'
import InitializationSaga from '@/app/containers/Initialization/sagas'
import projectManageSaga from '@/app/containers/ProjectManage/sagas'
import UserManageSaga from '@/app/containers/UserManage/sagas'
import sinkManageSaga from '@/app/containers/SinkManage/sagas'
import resourceManageSaga from '@/app/containers/ResourceManage/sagas'
import ToolSetSaga from '@/app/containers/toolSet/sagas'
import ConfigManageSaga from '@/app/containers/ConfigManage/sagas'

export default [
  ...appSagas,
  ...LoginSaga,
  ...InitializationSaga,
  ...projectManageSaga,
  ...sinkManageSaga,
  ...UserManageSaga,
  ...resourceManageSaga,
  ...ToolSetSaga,
  ...ConfigManageSaga
]
