import ZKManage from '@/app/components/ConfigManage/ZKManage/saga'
import GlobalConfig from '@/app/components/ConfigManage/GlobalConfig/saga'

export default [
  ...ZKManage,
  ...GlobalConfig
]
