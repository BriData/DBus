import { createSelector } from 'reselect'

// 获取list state
const ProjectHomeModel = () => createSelector(
  (state) => state.get('projectHomeReducer'),
  (state) => state.toJS()
)
export {
  ProjectHomeModel
}
