import {createSelector} from 'reselect'

// 获取 DataSource state
const ZKManageModel = () => createSelector(
  (state) => state.get('ZKManageReducer'),
  (state) => state.toJS()
)

const GlobalConfigModel = () => createSelector(
  (state) => state.get('GlobalConfigReducer'),
  (state) => state.toJS()
)

const DBAEncodeConfigModel = () => createSelector(
  (state) => state.get('DBAEncodeConfigReducer'),
  (state) => state.toJS()
)
export {
  ZKManageModel,
  GlobalConfigModel,
  DBAEncodeConfigModel
}
