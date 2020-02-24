// sink home
import SinkHomeSaga from '@/app/components/SinkManage/SinkList/saga'
import SinkerSchemaSaga from '@/app/components/SinkManage/SinkerSchema/saga'
import SinkerTableSaga from '@/app/components/SinkManage/SinkerTable/saga'

export default [
  ...SinkHomeSaga,
  ...SinkerSchemaSaga,
  ...SinkerTableSaga
]
