import {createSelector} from 'reselect'

const ControlMessageModel = () => createSelector(
  (state) => state.get('ControlMessageReducer'),
  (state) => state.toJS()
)

const GlobalFullpullModel = () => createSelector(
  (state) => state.get('GlobalFullpullReducer'),
  (state) => state.toJS()
)

const KafkaReaderModel = () => createSelector(
  (state) => state.get('KafkaReaderReducer'),
  (state) => state.toJS()
)

export {
  ControlMessageModel,
  GlobalFullpullModel,
  KafkaReaderModel
}
