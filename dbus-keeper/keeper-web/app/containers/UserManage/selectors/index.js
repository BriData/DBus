import { createSelector } from 'reselect'

// 获取 ProjectResource state
const userManageReducerModel = () => createSelector(
  (state) => state.get('userManageReducer'),
  (state) => state.toJS()
)
export {
  userManageReducerModel
}
