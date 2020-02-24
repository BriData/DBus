import {createSelector} from 'reselect'

// 获取 SinkHome
const sinkHomeModel = () => createSelector(
  (state) => state.get('sinkHomeReducer'),
  (state) => state.toJS()
)

// 获取 SinkerSchema
const sinkerSchemaModel = () => createSelector(
  (state) => state.get('sinkerSchemaReducer'),
  (state) => state.toJS()
)

// 获取 SinkerTsblr
const sinkerTableModel = () => createSelector(
  (state) => state.get('sinkerTableReducer'),
  (state) => state.toJS()
)
export {
  sinkHomeModel,
  sinkerSchemaModel,
  sinkerTableModel
}
