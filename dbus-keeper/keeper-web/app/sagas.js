import appSagas from './containers/App/sagas'
// 导入saga
import projectManageSaga from '@/app/containers/ProjectManage/sagas'
import UserManageSaga from '@/app/containers/UserManage/sagas'
import sinkManageSaga from '@/app/containers/SinkManage/sagas'
import resourceManageSaga from '@/app/containers/ResourceManage/sagas'
import ToolSetSaga from '@/app/containers/toolSet/sagas'
import ConfigManageSaga from '@/app/containers/ConfigManage/sagas'

export default [
  ...appSagas,
  ...projectManageSaga,
  ...sinkManageSaga,
  ...UserManageSaga,
  ...resourceManageSaga,
  ...ToolSetSaga,
  ...ConfigManageSaga
]
