import ControlMessageSaga from '@/app/components/toolSet/ControlMessage/saga'
import GlobalFullpullSaga from '@/app/components/toolSet/GlobalFullpull/saga'
import KafkaReaderSaga from '@/app/components/toolSet/KafkaReader/saga'

export default [
  ...ControlMessageSaga,
  ...GlobalFullpullSaga,
  ...KafkaReaderSaga
]
