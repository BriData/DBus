import { createSelector } from 'reselect'

// 获取list state
const InitializationModel = () => createSelector(
  (state) => state.get('initializationReducer'),
  (state) => state.toJS()
)
export {
  InitializationModel
}
