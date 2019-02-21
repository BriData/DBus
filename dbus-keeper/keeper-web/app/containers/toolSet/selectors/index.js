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

// 获取 DataSource state
const DataSourceModel = () => createSelector(
  (state) => state.get('DataSourceReducer'),
  (state) => state.toJS()
)
// 获取 JarManage state
const JarManageModel = () => createSelector(
  (state) => state.get('JarManageReducer'),
  (state) => state.toJS()
)

export {
  ControlMessageModel,
  GlobalFullpullModel,
  KafkaReaderModel,
  DataSourceModel,
  JarManageModel
}
