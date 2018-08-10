import { createSelector } from 'reselect'

// 获取 SinkHome
const sinkHomeModel = () => createSelector(
  (state) => state.get('sinkHomeReducer'),
  (state) => state.toJS()
)

export {
  sinkHomeModel
}
