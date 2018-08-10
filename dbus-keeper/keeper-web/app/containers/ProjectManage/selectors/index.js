import { createSelector } from 'reselect'

// 获取 ProjectHome state
const ProjectHomeModel = () => createSelector(
  (state) => state.get('projectHomeReducer'),
  (state) => state.toJS()
)
// 获取 ProjectResource state
const ProjectResourceModel = () => createSelector(
  (state) => state.get('projectResourceReducer'),
  (state) => state.toJS()
)
// 获取 ProjectTopology state
const ProjectTopologyModel = () => createSelector(
  (state) => state.get('projectTopologyReducer'),
  (state) => state.toJS()
)
// 获取 ProjectSummary state
const ProjectSummaryModel = () => createSelector(
  (state) => state.get('projectSummaryReducer'),
  (state) => state.toJS()
)
// 获取 ProjectTable state
const ProjectTableModel = () => createSelector(
  (state) => state.get('projectTableReducer'),
  (state) => state.toJS()
)
// 获取 ProjectFullpull state
const ProjectFullpullModel = () => createSelector(
  (state) => state.get('projectFullpullReducer'),
  (state) => state.toJS()
)
export {
  ProjectHomeModel,
  ProjectResourceModel,
  ProjectTopologyModel,
  ProjectSummaryModel,
  ProjectTableModel,
  ProjectFullpullModel
}
