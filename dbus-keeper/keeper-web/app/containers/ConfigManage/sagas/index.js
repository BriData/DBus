import ZKManage from '@/app/components/ConfigManage/ZKManage/saga'
import GlobalConfig from '@/app/components/ConfigManage/GlobalConfig/saga'
import DBAEncodeConfig from '@/app/components/ConfigManage/DBAEncodeConfig/saga'

export default [
  ...ZKManage,
  ...GlobalConfig,
  ...DBAEncodeConfig
]
