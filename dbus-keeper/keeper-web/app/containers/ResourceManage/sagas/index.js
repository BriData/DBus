// project summary
import EncodePluginManageSaga from '@/app/components/ResourceManage/EncodePluginManage/saga'
import JarManageSaga from '@/app/components/ResourceManage/JarManage/saga'
import DataSourceManageSaga from '@/app/components/ResourceManage/DataSourceManage/saga'
import DataSchemaManageSaga from '@/app/components/ResourceManage/DataSchemaManage/saga'
import DBusDataManageSaga from '@/app/components/ResourceManage/DBusDataManage/saga'
import EncodeManagerSaga from '@/app/components/ResourceManage/EncodeManager/saga'
import DataTableManageSaga from '@/app/components/ResourceManage/DataTableManage/saga'
import RuleGroupManageSaga from '@/app/components/RuleManage/RuleGroup/saga'
import DataSourceCreateSaga from '@/app/components/ResourceManage/DataSourceCreate/saga'

export default [
  ...EncodePluginManageSaga,
  ...JarManageSaga,
  ...DataSourceManageSaga,
  ...DataSchemaManageSaga,
  ...DataTableManageSaga,
  ...DBusDataManageSaga,
  ...EncodeManagerSaga,
  ...RuleGroupManageSaga,
  ...DataSourceCreateSaga
]
